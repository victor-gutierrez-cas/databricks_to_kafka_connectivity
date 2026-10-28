package com.example.kafka.entra;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Calls AWS STS GetWebIdentityToken (query API, version 2011-06-15) on the regional endpoint.
 * The global endpoint is not supported for this API.
 */
final class StsWebIdentityTokenClient {
    private static final Pattern TOKEN = Pattern.compile("<WebIdentityToken>\\s*([^<\\s]+)\\s*</WebIdentityToken>");
    private static final Pattern ERR_CODE = Pattern.compile("<Code>([^<]*)</Code>");
    private static final Pattern ERR_MSG = Pattern.compile("<Message>([^<]*)</Message>");

    private final String endpoint;
    private final String region;
    private final String audience;
    private final String signingAlgorithm;
    private final int durationSeconds;
    private final Http http;
    private final Retry retry;

    StsWebIdentityTokenClient(String endpoint, String region, String audience, String signingAlgorithm,
                              int durationSeconds, Http http, Retry retry) {
        if (endpoint.contains("://sts.amazonaws.com")) {
            throw new IllegalArgumentException("GetWebIdentityToken is not available on the STS global endpoint; "
                    + "use the regional endpoint https://sts.<region>.amazonaws.com");
        }
        this.endpoint = endpoint.endsWith("/") ? endpoint : endpoint + "/";
        this.region = region;
        this.audience = audience;
        this.signingAlgorithm = signingAlgorithm;
        this.durationSeconds = durationSeconds;
        this.http = http;
        this.retry = retry;
    }

    String requestBody() {
        return Http.form(
                "Action", "GetWebIdentityToken",
                "Version", "2011-06-15",
                "Audience.member.1", audience,
                "SigningAlgorithm", signingAlgorithm,
                "DurationSeconds", Integer.toString(durationSeconds));
    }

    String getWebIdentityToken(AwsCredentials creds) throws IOException {
        String body = requestBody();
        return retry.run("AWS STS GetWebIdentityToken", () -> {
            Http.Response r = http.request("POST", endpoint,
                    SigV4.signFormPost(endpoint, body, region, "sts", creds, ZonedDateTime.now()), body);
            if (r.status == 200) {
                Matcher m = TOKEN.matcher(r.body);
                if (!m.find()) throw new IOException("STS response did not contain WebIdentityToken");
                return m.group(1);
            }
            String code = first(ERR_CODE, r.body);
            String msg = first(ERR_MSG, r.body);
            String detail = "AWS STS GetWebIdentityToken failed: HTTP " + r.status
                    + (code == null ? "" : " " + code) + (msg == null ? "" : ": " + msg)
                    + hint(code);
            boolean retryable = r.status >= 500 || r.status == 429 || "Throttling".equals(code);
            throw retryable ? new Retry.RetryableException(detail) : new IOException(detail);
        });
    }

    private static String hint(String code) {
        if ("OutboundWebIdentityFederationDisabled".equals(code)) {
            return " (enable it once per account: aws iam enable-outbound-web-identity-federation)";
        }
        if ("AccessDenied".equals(code)) {
            return " (the instance profile role needs sts:GetWebIdentityToken; check sts:IdentityTokenAudience / "
                    + "sts:DurationSeconds conditions)";
        }
        return "";
    }

    private static String first(Pattern p, String s) {
        Matcher m = p.matcher(s);
        return m.find() ? m.group(1) : null;
    }
}
