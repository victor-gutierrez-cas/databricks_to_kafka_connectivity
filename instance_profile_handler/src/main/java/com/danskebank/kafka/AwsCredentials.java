package com.danskebank.kafka;

/** Temporary AWS credentials. toString never prints secret material. */
final class AwsCredentials {
    final String accessKeyId;
    final String secretAccessKey;
    final String sessionToken;   // null for long term keys
    final long expiresAtMs;      // Long.MAX_VALUE when unknown (env vars)

    AwsCredentials(String accessKeyId, String secretAccessKey, String sessionToken, long expiresAtMs) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
        this.expiresAtMs = expiresAtMs;
    }

    @Override
    public String toString() {
        return "AwsCredentials[accessKeyId=" + (accessKeyId == null ? "null"
                : accessKeyId.substring(0, Math.min(4, accessKeyId.length())) + "****") + "]";
    }
}
