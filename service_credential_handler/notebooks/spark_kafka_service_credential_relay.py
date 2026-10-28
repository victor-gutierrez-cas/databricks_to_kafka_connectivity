# Databricks notebook source
# MAGIC %md
# MAGIC # Example: Spark Kafka source with a UC service credential (inline token relay)
# MAGIC
# MAGIC Executors can't read a UC service credential, and on UC compute the JVM can't read `/Volumes`, so:
# MAGIC 1. The **driver** uses the service credential: STS `GetWebIdentityToken` -> Entra -> access token.
# MAGIC 2. The token goes **inline in the Kafka JAAS config** (`token.value`); Spark ships it to the executors, where
# MAGIC    `com.example.kafka.entra.relay.FileTokenOAuthBearerCallbackHandler` (`confluent-entra-handler-service-credential.jar`)
# MAGIC    hands it to Kafka with `logicalCluster`.
# MAGIC 3. A token can't change inside a running query, so the stream is **restarted from its checkpoint** with a fresh
# MAGIC    token before the old one expires.
# MAGIC
# MAGIC Needs dedicated (single user) classic compute with the v2 relay JAR. No instance profile needed.
# MAGIC The token (~1 hour bearer) travels in the query options: treat query plans and logs as sensitive.

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade boto3 botocore
# MAGIC # GetWebIdentityToken needs a recent boto3.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("service_credential", "<service-credential-name>")
dbutils.widgets.text("aws_region", "eu-central-1")
dbutils.widgets.text("tenant_id", "<entra-tenant-id>")
dbutils.widgets.text("client_id", "<entra-app-client-id>")
dbutils.widgets.text("scope", "api://<entra-app-client-id>/.default")
dbutils.widgets.text("bootstrap", "<bootstrap-server>:9092")
dbutils.widgets.text("logical_cluster", "<lkc-xxxxx>")
dbutils.widgets.text("identity_pool_id", "<pool-xxxxx>")
dbutils.widgets.text("topic", "<topic>")
dbutils.widgets.text("target_table", "<catalog>.<schema>.kafka_sc_relay_events")
dbutils.widgets.text("checkpoint", "/Volumes/<catalog>/<schema>/checkpoints/kafka_sc_relay_stream")
dbutils.widgets.text("restart_minutes", "45")

w = {k: dbutils.widgets.get(k).strip() for k in
     ("service_credential", "aws_region", "tenant_id", "client_id", "scope", "bootstrap", "logical_cluster",
      "identity_pool_id", "topic", "target_table", "checkpoint", "restart_minutes")}
scope = w["scope"] or f"api://{w['client_id']}/.default"

# COMMAND ----------

# MAGIC %md ## Driver: mint Entra tokens with the service credential

# COMMAND ----------

import base64, json, time
import boto3, requests

def claims(jwt: str) -> dict:
    p = jwt.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(p + "=" * (-len(p) % 4)))

session = boto3.Session(botocore_session=dbutils.credentials.getServiceCredentialsProvider(w["service_credential"]),
                        region_name=w["aws_region"])
sts = session.client("sts", endpoint_url=f"https://sts.{w['aws_region']}.amazonaws.com")
print("STS caller (service credential session):", sts.get_caller_identity()["Arn"])

def mint_entra_token() -> str:
    aws_jwt = sts.get_web_identity_token(Audience=["api://AzureADTokenExchange"], SigningAlgorithm="RS256",
                                         DurationSeconds=300)["WebIdentityToken"]
    r = requests.post(f"https://login.microsoftonline.com/{w['tenant_id']}/oauth2/v2.0/token", timeout=30, data={
        "grant_type": "client_credentials", "client_id": w["client_id"], "scope": scope,
        "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        "client_assertion": aws_jwt})
    if r.status_code != 200:
        raise RuntimeError(f"Entra HTTP {r.status_code}: {r.text[:500]}")
    return r.json()["access_token"]

def kafka_options(token: str) -> dict:
    for v in (token, w["logical_cluster"], w["identity_pool_id"]):
        assert '"' not in v and ";" not in v, "JAAS values can't contain quotes or semicolons"
    opts = [f'token.value="{token}"', f'extension_logicalCluster="{w["logical_cluster"]}"']
    if w["identity_pool_id"]:
        opts.append(f'extension_identityPoolId="{w["identity_pool_id"]}"')
    return {
        "kafka.bootstrap.servers": w["bootstrap"],
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "OAUTHBEARER",
        "kafka.sasl.login.callback.handler.class": "com.example.kafka.entra.relay.FileTokenOAuthBearerCallbackHandler",
        "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule "
                                  "required " + " ".join(opts) + ";",
    }

tok = mint_entra_token()
c = claims(tok)
print("Entra token:", {k: c.get(k) for k in ("iss", "aud", "sub", "ver")},
      "expires", time.strftime("%H:%M:%S", time.localtime(c["exp"])))

# COMMAND ----------

# MAGIC %md ## Batch read

# COMMAND ----------

df = (spark.read.format("kafka").options(**kafka_options(mint_entra_token()))
      .option("subscribe", w["topic"])
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load())
display(df.selectExpr("topic", "partition", "offset", "timestamp", "CAST(value AS STRING) AS value").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming read into a Delta table, restarted with a fresh token every `restart_minutes`
# MAGIC Interrupt the cell to stop. Offsets survive restarts through the checkpoint.

# COMMAND ----------

restart_seconds = int(w["restart_minutes"]) * 60
cycle = 0
try:
    while True:
        cycle += 1
        token = mint_entra_token()
        q = (spark.readStream.format("kafka").options(**kafka_options(token))
             .option("subscribe", w["topic"])
             .option("startingOffsets", "earliest")
             .load()
             .selectExpr("topic", "partition", "offset", "timestamp", "CAST(value AS STRING) AS value")
             .writeStream
             .option("checkpointLocation", w["checkpoint"])
             .trigger(processingTime="30 seconds")
             .toTable(w["target_table"]))
        print(time.strftime("%H:%M:%S"), f"cycle {cycle}: stream started, token exp",
              time.strftime("%H:%M:%S", time.localtime(claims(token)["exp"])))
        q.awaitTermination(restart_seconds)
        if q.exception():
            raise q.exception()
        q.stop()
        print(time.strftime("%H:%M:%S"), f"cycle {cycle}: stopped for token refresh, rows so far:",
              spark.table(w["target_table"]).count())
finally:
    for s in spark.streams.active:
        s.stop()

# COMMAND ----------

display(spark.table(w["target_table"]).orderBy("offset"))
