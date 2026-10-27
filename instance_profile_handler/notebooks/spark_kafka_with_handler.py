# Databricks notebook source
# MAGIC %md
# MAGIC # Example: Spark Structured Streaming to Confluent Cloud with the passwordless handler
# MAGIC
# MAGIC **Run on:** dedicated (single user) classic compute with the instance profile attached, and the handler JAR
# MAGIC installed as a cluster library from a UC volume (restart the cluster after installing).
# MAGIC Custom callback handlers are blocked on standard access mode and serverless.
# MAGIC
# MAGIC Nothing here is a secret: the JAAS config only carries IDs.

# COMMAND ----------

dbutils.widgets.text("aws_region", "eu-central-1")
dbutils.widgets.text("tenant_id", "")
dbutils.widgets.text("client_id", "")
dbutils.widgets.text("scope", "", "blank = <client_id>/.default")
dbutils.widgets.text("bootstrap", "", "pkc-xxxxx.eu-central-1.aws.confluent.cloud:9092")
dbutils.widgets.text("logical_cluster", "", "lkc-xxxxx")
dbutils.widgets.text("identity_pool_id", "", "optional; blank = auto pool mapping")
dbutils.widgets.text("topic", "")
dbutils.widgets.text("checkpoint", "", "UC volume path for the streaming checkpoint")

w = {k: dbutils.widgets.get(k).strip() for k in
     ("aws_region", "tenant_id", "client_id", "scope", "bootstrap", "logical_cluster", "identity_pool_id", "topic",
      "checkpoint")}
assert all(w[k] for k in ("tenant_id", "client_id", "bootstrap", "logical_cluster", "topic")), "Fill in the widgets"

# COMMAND ----------

def jaas_value(v: str) -> str:
    assert '"' not in v and ";" not in v, "JAAS values can't contain quotes or semicolons"
    return v


opts = [
    f'tenantId="{jaas_value(w["tenant_id"])}"',
    f'clientId="{jaas_value(w["client_id"])}"',
    f'aws.region="{jaas_value(w["aws_region"])}"',
    f'extension_logicalCluster="{jaas_value(w["logical_cluster"])}"',
]
if w["scope"]:
    opts.append(f'scope="{jaas_value(w["scope"])}"')
if w["identity_pool_id"]:
    opts.append(f'extension_identityPoolId="{jaas_value(w["identity_pool_id"])}"')

jaas = ("kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
        + " ".join(opts) + ";")

kafka_options = {
    "kafka.bootstrap.servers": w["bootstrap"],
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "OAUTHBEARER",
    "kafka.sasl.login.callback.handler.class": "com.danskebank.kafka.AwsFederatedEntraOAuthBearerCallbackHandler",
    "kafka.sasl.jaas.config": jaas,
}
print(jaas)

# COMMAND ----------

# MAGIC %md ## Batch read (quick connectivity and auth check)

# COMMAND ----------

df = (spark.read.format("kafka").options(**kafka_options)
      .option("subscribe", w["topic"])
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load())
display(df.selectExpr("topic", "partition", "offset", "timestamp", "CAST(value AS STRING) AS value").limit(20))

# COMMAND ----------

# MAGIC %md ## Streaming read (run past one hour to confirm token refresh)

# COMMAND ----------

# Workspaces that don't allow implicit temp checkpoints need an explicit path
assert w["checkpoint"], "Set the checkpoint widget, e.g. /Volumes/<catalog>/<schema>/<volume>/checkpoints/kafka_poc"
stream = (spark.readStream.format("kafka").options(**kafka_options)
          .option("subscribe", w["topic"])
          .option("startingOffsets", "earliest")
          .load()
          .selectExpr("topic", "partition", "offset", "timestamp", "CAST(value AS STRING) AS value"))
display(stream, checkpointLocation=w["checkpoint"])
