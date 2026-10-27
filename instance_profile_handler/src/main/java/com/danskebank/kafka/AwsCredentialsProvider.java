package com.danskebank.kafka;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

/**
 * Resolves AWS credentials without any stored secret:
 *   env:  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN (local testing)
 *   imds: EC2 instance metadata (IMDSv2) for the instance profile attached to the Databricks cluster
 *   auto: env if set, otherwise imds
 * IMDS credentials are cached until 5 minutes before they expire.
 */
final class AwsCredentialsProvider {
    private static final long REFRESH_MARGIN_MS = 5 * 60 * 1000L;
    private static final String CREDS_PATH = "/latest/meta-data/iam/security-credentials/";

    private final String source;
    private final String imdsEndpoint;
    private final Http http;
    private AwsCredentials cached;

    AwsCredentialsProvider(String source, String imdsEndpoint, Http http) {
        if (!"auto".equals(source) && !"env".equals(source) && !"imds".equals(source)) {
            throw new IllegalArgumentException("aws.credentials.source must be auto, env or imds");
        }
        this.source = source;
        this.imdsEndpoint = imdsEndpoint.endsWith("/") ? imdsEndpoint.substring(0, imdsEndpoint.length() - 1)
                : imdsEndpoint;
        this.http = http;
    }

    synchronized AwsCredentials get() throws IOException {
        if (!"imds".equals(source)) {
            AwsCredentials env = fromEnv();
            if (env != null) return env;
            if ("env".equals(source)) {
                throw new IOException("aws.credentials.source=env but AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are not set");
            }
        }
        if (cached == null || System.currentTimeMillis() > cached.expiresAtMs - REFRESH_MARGIN_MS) {
            cached = fromImds();
        }
        return cached;
    }

    private static AwsCredentials fromEnv() {
        String ak = System.getenv("AWS_ACCESS_KEY_ID");
        String sk = System.getenv("AWS_SECRET_ACCESS_KEY");
        if (!AwsFederatedEntraOAuthBearerCallbackHandler.present(ak)
                || !AwsFederatedEntraOAuthBearerCallbackHandler.present(sk)) {
            return null;
        }
        String st = System.getenv("AWS_SESSION_TOKEN");
        return new AwsCredentials(ak, sk, AwsFederatedEntraOAuthBearerCallbackHandler.present(st) ? st : null,
                Long.MAX_VALUE);
    }

    private AwsCredentials fromImds() throws IOException {
        Http.Response tok = http.request("PUT", imdsEndpoint + "/latest/api/token",
                Map.of("X-aws-ec2-metadata-token-ttl-seconds", "21600"), null);
        if (tok.status != 200) {
            throw new IOException("IMDSv2 token request failed with HTTP " + tok.status
                    + " (is an instance profile attached and IMDS reachable from this cluster?)");
        }
        Map<String, String> h = Map.of("X-aws-ec2-metadata-token", tok.body.trim());

        Http.Response roles = http.request("GET", imdsEndpoint + CREDS_PATH, h, null);
        if (roles.status != 200 || roles.body.isBlank()) {
            throw new IOException("No IAM role found in instance metadata (HTTP " + roles.status
                    + "). Attach an instance profile to the cluster.");
        }
        String role = roles.body.trim().split("\\R")[0].trim();

        Http.Response c = http.request("GET", imdsEndpoint + CREDS_PATH + role, h, null);
        if (c.status != 200) {
            throw new IOException("Instance profile credential request failed with HTTP " + c.status);
        }
        String code = Json.string(c.body, "Code");
        if (code != null && !"Success".equals(code)) {
            throw new IOException("Instance profile credentials not available, IMDS Code=" + code);
        }
        String ak = Json.string(c.body, "AccessKeyId");
        String sk = Json.string(c.body, "SecretAccessKey");
        String st = Json.string(c.body, "Token");
        String exp = Json.string(c.body, "Expiration");
        if (ak == null || sk == null) {
            throw new IOException("Instance profile credential response is missing AccessKeyId/SecretAccessKey");
        }
        long expMs = exp == null ? System.currentTimeMillis() + 15 * 60 * 1000L : Instant.parse(exp).toEpochMilli();
        return new AwsCredentials(ak, sk, st, expMs);
    }
}
