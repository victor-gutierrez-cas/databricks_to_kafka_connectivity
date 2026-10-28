# Token relay handler: Spark Kafka source with a Unity Catalog service credential

Custom Kafka `OAUTHBEARER` login callback handler for the **Databricks shaded Kafka client**, used when the AWS IAM role comes from a **Unity Catalog service credential** instead of an instance profile.

Executors can't read a service credential, so the driver does the token work and the handler only relays the result:

```
Driver (notebook)
  UC service credential -> STS GetWebIdentityToken (AWS signed JWT)
  -> Entra ID client_credentials + client_assertion -> Entra access token
  -> passed inline in kafka.sasl.jaas.config (token.value)
Executors (Spark Kafka source)
  com.example.kafka.entra.relay.FileTokenOAuthBearerCallbackHandler
  -> hands the token to Kafka with logicalCluster (+ identityPoolId)
  -> Confluent validates the Entra token, identity pool, RBAC
```

The token can't change inside a running query, so the example notebook restarts the stream from its checkpoint about every 45 minutes with a fresh token. The token (about one hour) travels in the query configuration.

## Requirements

* Dedicated (single user) classic compute; custom handlers are blocked on standard access mode and serverless.
* A Unity Catalog service credential for an IAM role allowed to call `sts:GetWebIdentityToken` (audience `api://AzureADTokenExchange`), and `ACCESS` on it.
* Entra ID app registration with a federated credential for that role; Confluent Cloud with Entra ID as identity provider and an identity pool (Standard cluster or higher).
* Egress to AWS STS and Entra ID (443) and Confluent (9092).

## Build and deploy

See **Build and deploy** in the top level `README.md` for the full steps (getting the shaded Kafka JAR off a cluster,
building in your pipeline or in the workspace, installing). In short:

```bash
./build.sh /path/to/kafkashaded.jar
# -> build/confluent-entra-handler-service-credential.jar (only com.example.kafka.entra.relay classes)
```

Upload the JAR to a UC volume, install it as a library on a dedicated cluster and restart the cluster.

It uses a different package than the instance profile handler, but don't install two versions of this JAR on the same cluster.

## JAAS options

| Option | Required | Notes |
|---|---|---|
| `token.value` | one of the two | Entra access token, set by the driver when starting the query |
| `token.file` | one of the two | File every executor can read. Note: on UC compute the JVM can't read `/Volumes` paths |
| `token.minRemainingSeconds` | no (300) | Rejects tokens closer to expiry than this |
| `token.maxWaitSeconds` | no (120) | File mode only: wait for a fresh token |
| `extension_logicalCluster` | yes | Confluent `lkc-xxxxx` |
| `extension_identityPoolId` | no | Confluent `pool-xxxxx` |

`clientSecret`, `password` and `username` are rejected.

## Example

`notebooks/spark_kafka_service_credential_relay.py`: mints the token with the service credential, runs a batch read, then a stream into a Delta table with the restart loop. Fill in the widgets.
