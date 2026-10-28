package com.example.kafka.entra;

// DATABRICKS-SHADED Kafka interfaces. A custom SASL callback handler MUST implement the
// shaded interface (not vanilla org.apache.kafka.*), or the shaded client rejects it.
import kafkashaded.org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import kafkashaded.org.apache.kafka.common.security.auth.SaslExtensions;
import kafkashaded.org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Passwordless Entra ID OAUTHBEARER callback handler for the Databricks-SHADED Kafka client,
 * for Databricks on AWS talking to Confluent Cloud.
 *
 * Flow on every Kafka login / re-login:
 *   1. Read AWS credentials of the cluster's instance profile (IMDSv2), or env vars.
 *   2. Call AWS STS GetWebIdentityToken (SigV4 signed, regional endpoint) to get an AWS-signed JWT
 *      for the IAM role (aud = api://AzureADTokenExchange, RS256).
 *   3. Exchange it at Entra ID as a federated client assertion (client_credentials grant with
 *      client_assertion) for an Entra access token. Entra validates the AWS JWT against the
 *      federated identity credential on the app registration.
 *   4. Hand the Entra token plus the Confluent SASL extensions (logicalCluster, identityPoolId)
 *      to the Kafka client. Confluent validates the Entra token.
 *
 * No secret is configured or stored. Configuring clientSecret, password or username is rejected.
 * All values come from the JAAS entry options; tokens and credentials are never logged.
 *
 * JAAS options:
 *   tenantId                    required (unless token.endpoint.url is set)
 *   clientId                    required, Entra app registration (client) ID with the federated credential
 *   scope                       optional, default "<clientId>/.default"
 *   token.endpoint.url          optional, default https://login.microsoftonline.com/<tenantId>/oauth2/v2.0/token
 *   aws.region                  required unless AWS_REGION / AWS_DEFAULT_REGION is set
 *   aws.audience                optional, default api://AzureADTokenExchange
 *   aws.signingAlgorithm        optional, RS256 (default, the algorithm Entra supports) or ES384
 *   aws.durationSeconds         optional, 60..3600, default 300
 *   aws.sts.endpoint            optional, default https://sts.<region>.amazonaws.com (e.g. a VPC endpoint)
 *   aws.credentials.source      optional, auto (default: env vars, then IMDSv2) | env | imds
 *   aws.imds.endpoint           optional, default http://169.254.169.254
 *   http.connectTimeoutMs       optional, default 10000
 *   http.readTimeoutMs          optional, default 30000
 *   retry.maxAttempts           optional, default 4 (STS and Entra calls, retryable errors only)
 *   retry.initialBackoffMs      optional, default 1000 (doubles per attempt)
 *   extension_logicalCluster    required by Confluent Cloud (lkc-xxxxx)
 *   extension_identityPoolId    optional (omit to use Confluent auto pool mapping)
 */
public class AwsFederatedEntraOAuthBearerCallbackHandler implements AuthenticateCallbackHandler {

    static final String DEFAULT_AUDIENCE = "api://AzureADTokenExchange";

    private Map<String, String> options = Collections.emptyMap();
    private AwsCredentialsProvider credentialsProvider;
    private StsWebIdentityTokenClient stsClient;
    private EntraFederatedTokenClient entraClient;

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism,
                          List<AppConfigurationEntry> jaasConfigEntries) {
        if (!"OAUTHBEARER".equalsIgnoreCase(saslMechanism)) {
            throw new IllegalStateException("Unexpected SASL mechanism: " + saslMechanism);
        }
        if (jaasConfigEntries == null || jaasConfigEntries.size() != 1) {
            throw new IllegalArgumentException("Expected exactly one JAAS entry");
        }
        Map<String, String> opts = new LinkedHashMap<>();
        for (Map.Entry<String, ?> e : jaasConfigEntries.get(0).getOptions().entrySet()) {
            opts.put(e.getKey(), String.valueOf(e.getValue()));
        }
        configure(opts);
    }

    /** Visible for tests: configure from a plain option map. */
    void configure(Map<String, String> opts) {
        for (String forbidden : new String[]{"clientSecret", "password", "username"}) {
            if (present(opts.get(forbidden))) {
                throw new IllegalArgumentException(
                        "JAAS option '" + forbidden + "' is not allowed: this handler is passwordless");
            }
        }
        this.options = opts;

        Http http = new Http(intOpt("http.connectTimeoutMs", 10_000), intOpt("http.readTimeoutMs", 30_000));
        Retry retry = new Retry(intOpt("retry.maxAttempts", 4), intOpt("retry.initialBackoffMs", 1_000));

        String region = firstPresent(opts.get("aws.region"), System.getenv("AWS_REGION"),
                System.getenv("AWS_DEFAULT_REGION"));
        if (region == null) {
            throw new IllegalArgumentException("Missing required JAAS option: aws.region");
        }
        String stsEndpoint = firstPresent(opts.get("aws.sts.endpoint"), "https://sts." + region + ".amazonaws.com");
        String algorithm = opts.getOrDefault("aws.signingAlgorithm", "RS256");
        if (!"RS256".equals(algorithm) && !"ES384".equals(algorithm)) {
            throw new IllegalArgumentException("aws.signingAlgorithm must be RS256 or ES384");
        }
        int duration = intOpt("aws.durationSeconds", 300);
        if (duration < 60 || duration > 3600) {
            throw new IllegalArgumentException("aws.durationSeconds must be between 60 and 3600");
        }

        String tokenEndpoint = opts.get("token.endpoint.url");
        if (!present(tokenEndpoint)) {
            tokenEndpoint = "https://login.microsoftonline.com/" + required("tenantId") + "/oauth2/v2.0/token";
        }
        String clientId = required("clientId");
        String scope = firstPresent(opts.get("scope"), clientId + "/.default");

        this.credentialsProvider = new AwsCredentialsProvider(
                opts.getOrDefault("aws.credentials.source", "auto"),
                opts.getOrDefault("aws.imds.endpoint", "http://169.254.169.254"), http);
        this.stsClient = new StsWebIdentityTokenClient(stsEndpoint, region,
                firstPresent(opts.get("aws.audience"), DEFAULT_AUDIENCE), algorithm, duration, http, retry);
        this.entraClient = new EntraFederatedTokenClient(tokenEndpoint, clientId, scope, http, retry);
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback cb : callbacks) {
            if (cb instanceof OAuthBearerTokenCallback) {
                OAuthBearerTokenCallback tc = (OAuthBearerTokenCallback) cb;
                try {
                    tc.token(acquireToken());
                } catch (Exception ex) {
                    tc.error("token_retrieval_failed", ex.getMessage(), null);
                }
            } else if (cb instanceof SaslExtensionsCallback) {
                SaslExtensionsCallback ec = (SaslExtensionsCallback) cb;
                Map<String, String> ext = new LinkedHashMap<>();
                putIfPresent(ext, "logicalCluster", options.get("extension_logicalCluster"));
                putIfPresent(ext, "identityPoolId", options.get("extension_identityPoolId"));
                ec.extensions(new SaslExtensions(ext));
            } else {
                throw new UnsupportedCallbackException(cb);
            }
        }
    }

    /** Steps 1 to 3: AWS credentials, AWS JWT, Entra access token. */
    OAuthBearerToken acquireToken() throws IOException {
        AwsCredentials creds = credentialsProvider.get();
        String awsJwt = stsClient.getWebIdentityToken(creds);
        String accessToken = entraClient.exchange(awsJwt);
        long[] times = Jwt.expiryAndIssuedAtMs(accessToken);
        return new SimpleToken(accessToken, Jwt.claim(accessToken, "sub"), times[0], times[1]);
    }

    @Override
    public void close() { /* no-op */ }

    private String required(String key) {
        String v = options.get(key);
        if (!present(v)) {
            throw new IllegalArgumentException("Missing required JAAS option: " + key);
        }
        return v;
    }

    private int intOpt(String key, int def) {
        String v = options.get(key);
        if (!present(v)) return def;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("JAAS option " + key + " must be an integer");
        }
    }

    static boolean present(String v) {
        return v != null && !v.isEmpty() && !"null".equals(v);
    }

    private static String firstPresent(String... values) {
        for (String v : values) if (present(v)) return v;
        return null;
    }

    private static void putIfPresent(Map<String, String> m, String k, String v) {
        if (present(v)) m.put(k, v);
    }

    /** Minimal OAuthBearerToken backed by the raw Entra JWT. */
    static final class SimpleToken implements OAuthBearerToken {
        private final String value;
        private final String principal;
        private final long lifetimeMs;
        private final long startTimeMs;

        SimpleToken(String value, String principal, long lifetimeMs, long startTimeMs) {
            this.value = value;
            this.principal = principal == null ? "unknown" : principal;
            this.lifetimeMs = lifetimeMs;
            this.startTimeMs = startTimeMs;
        }

        @Override public String value() { return value; }
        @Override public Set<String> scope() { return Collections.emptySet(); }
        @Override public long lifetimeMs() { return lifetimeMs; }
        @Override public String principalName() { return principal; }
        @Override public Long startTimeMs() { return startTimeMs; }
        @Override public String toString() { return "SimpleToken[principal=" + principal + "]"; }
    }
}
