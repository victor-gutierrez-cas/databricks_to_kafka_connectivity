# Passwordless Entra ID handler: Databricks on AWS to Confluent Cloud

Custom Kafka `OAUTHBEARER` login callback handler for the **Databricks shaded Kafka client**. It authenticates Spark Structured Streaming to Confluent Cloud with Entra ID as IdP **without any stored secret**, by federating the cluster's AWS IAM role into Entra.

Built on the pattern of the Databricks Entra OAuth handler for Confluent Kafka shared earlier: same shaded interfaces, same Confluent SASL extensions, same dependency free approach (JDK only). The difference is where the credential comes from: an AWS signed JWT instead of a client secret or password.

**Status:** tested end to end on Databricks (dedicated classic compute, instance profile) against Entra ID and Confluent Cloud, batch and streaming. Reference implementation: review and build it in your own pipeline.

## Flow (on every Kafka login and re-login)

```
Dedicated classic cluster, instance profile = IAM role
 1. IMDSv2                 -> temporary credentials of the instance profile role (cached until 5 min before expiry)
 2. STS GetWebIdentityToken (regional endpoint, SigV4 signed, aud api://AzureADTokenExchange, RS256, 300 s)
                           -> AWS signed JWT, sub = role ARN
 3. Entra token endpoint   client_credentials + client_assertion = AWS JWT
                           -> Entra validates the JWT against the app's federated credential -> access token
 4. Kafka client           Entra token + SASL extensions logicalCluster (+ optional identityPoolId)
                           -> Confluent validates the Entra token, maps it to an identity pool, applies RBAC
```

The handler fetches and hands over tokens; it validates nothing. Entra validates the AWS JWT, Confluent validates the Entra token.

## Requirements

* Databricks on AWS, **dedicated (single user) classic compute** with an **instance profile**. Custom handlers are blocked on standard access mode and serverless (`RESTRICTED_STREAMING_OPTION_PERMISSION_ENFORCED`). Build against the shaded Kafka JAR of the runtime you use; Java 17.
* Cluster egress to `sts.<region>.amazonaws.com` (or an STS VPC endpoint) and `login.microsoftonline.com`, plus Confluent.
* Confluent Cloud **Standard or higher** cluster (identity pools need RBAC, which Basic lacks).

## Setup

**AWS**
1. Enable outbound identity federation once per account: `aws iam enable-outbound-web-identity-federation`. Note the issuer URL (`aws iam get-outbound-web-identity-federation-info`).
2. Allow the instance profile role to call STS, ideally pinned:
   ```json
   {"Effect": "Allow", "Action": "sts:GetWebIdentityToken", "Resource": "*",
    "Condition": {"ForAllValues:StringEquals": {"sts:IdentityTokenAudience": "api://AzureADTokenExchange"},
                  "NumericLessThanEquals": {"sts:DurationSeconds": 300}}}
   ```

**Entra ID** (on the app registration / Entra SP used for Confluent)
1. Certificates & secrets, Federated credentials, Add credential, scenario **Other issuer**:
   issuer = AWS account issuer URL, subject = role ARN (exact, no wildcards), audience = `api://AzureADTokenExchange`.
2. Expose the API Confluent expects (Application ID URI) and set `accessTokenAcceptedVersion` to 2 if Confluent's identity provider expects v2 tokens.
3. No client secret or certificate is needed on this app for this flow.

**Confluent Cloud**
1. Entra ID as OIDC identity provider (issuer and JWKS from the tenant).
2. Identity pool with a claim filter matching the token (for example `claims.aud` and `claims.sub` / `claims.appid`), plus RBAC role bindings on the topics.

## Build and deploy

See **Build and deploy** in the top level `README.md` for the full steps (getting the shaded Kafka JAR off a cluster,
building in your pipeline or in the workspace, installing). In short:

```bash
./build.sh /path/to/kafkashaded.jar
# -> build/confluent-entra-handler-instance-profile.jar (only com.danskebank.kafka classes)
```

Upload the JAR to a UC volume, install it as a library on a dedicated cluster and restart the cluster.

## Kafka options

```python
jaas = ('kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required '
        'tenantId="<tenant-id>" clientId="<app-client-id>" scope="<app-id-uri>/.default" '
        'aws.region="eu-central-1" extension_logicalCluster="lkc-xxxxx";')
opts = {
    "kafka.bootstrap.servers": "pkc-xxxxx.eu-central-1.aws.confluent.cloud:9092",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "OAUTHBEARER",
    "kafka.sasl.login.callback.handler.class": "com.danskebank.kafka.AwsFederatedEntraOAuthBearerCallbackHandler",
    "kafka.sasl.jaas.config": jaas,
}
```

| JAAS option | Required | Default | Notes |
|---|---|---|---|
| `tenantId` | yes* | | *unless `token.endpoint.url` is set |
| `clientId` | yes | | App registration with the federated credential |
| `scope` | no | `<clientId>/.default` | Confluent app ID URI + `/.default` |
| `token.endpoint.url` | no | `https://login.microsoftonline.com/<tenantId>/oauth2/v2.0/token` | |
| `aws.region` | yes* | | *or `AWS_REGION` / `AWS_DEFAULT_REGION` |
| `aws.audience` | no | `api://AzureADTokenExchange` | Must match the federated credential audience |
| `aws.signingAlgorithm` | no | `RS256` | Entra supports RS256 only; ES384 allowed for other targets |
| `aws.durationSeconds` | no | `300` | 60 to 3600; only used for the exchange |
| `aws.sts.endpoint` | no | `https://sts.<region>.amazonaws.com` | e.g. an STS VPC endpoint. Global endpoint rejected |
| `aws.credentials.source` | no | `auto` | `auto` (env vars, then IMDSv2), `env`, `imds` |
| `aws.imds.endpoint` | no | `http://169.254.169.254` | |
| `http.connectTimeoutMs` / `http.readTimeoutMs` | no | 10000 / 30000 | |
| `retry.maxAttempts` / `retry.initialBackoffMs` | no | 4 / 1000 | Retries 5xx, 429, STS throttling, Entra `AADSTS70021` only |
| `extension_logicalCluster` | yes (Confluent Cloud) | | `lkc-xxxxx` |
| `extension_identityPoolId` | no | | Omit for Confluent auto pool mapping |

`clientSecret`, `password` and `username` are **rejected**: the handler refuses to run with a secret.

## Files

| Path | What |
|---|---|
| `src/main/java/com/danskebank/kafka/AwsFederatedEntraOAuthBearerCallbackHandler.java` | Handler (Kafka callbacks, options, SASL extensions) |
| `.../AwsCredentialsProvider.java`, `AwsCredentials.java` | Env vars or IMDSv2 instance profile credentials |
| `.../SigV4.java` | JDK only AWS Signature V4 for the STS form POST |
| `.../StsWebIdentityTokenClient.java` | STS `GetWebIdentityToken` call and error mapping |
| `.../EntraFederatedTokenClient.java` | Entra client credentials grant with `client_assertion` |
| `.../Http.java`, `Retry.java`, `Json.java`, `Jwt.java` | Small helpers |
| `notebooks/spark_kafka_with_handler.py` | Example: Spark batch and streaming read with the handler |

## Troubleshooting (as reported in the Kafka auth error)

| Message | Cause |
|---|---|
| `IMDSv2 token request failed` / `No IAM role found` | No instance profile on the cluster, or IMDS not reachable (wrong access mode) |
| `OutboundWebIdentityFederationDisabled` | Enable it on the AWS account |
| `AccessDenied` from STS | Role lacks `sts:GetWebIdentityToken` or a policy condition doesn't match |
| `AADSTS70021` (after retries) | Federated credential not propagated or no issuer/subject/audience match |
| `AADSTS700213` / `AADSTS700211` | Subject (role ARN) or issuer mismatch on the federated credential |
| Kafka `SaslAuthenticationException` after a token was issued | Confluent side: identity provider, identity pool filter, `logicalCluster`, or RBAC |

## Known limits and open points

* Dedicated classic only; not serverless (no instance profile there, and custom handlers are blocked). The upcoming custom JAR auth on UC Kafka connections might lift the handler restriction, but where AWS credentials would come from on serverless is unknown.
* Each executor's Kafka client logs in independently (STS + Entra call per login). Fine for normal executor counts; consider this for very large clusters and Entra throttling.
* Not yet validated: Entra accepting AWS STS tokens in practice, Danske Conditional Access for workload identities, behaviour on the exact DBR Danske runs.
