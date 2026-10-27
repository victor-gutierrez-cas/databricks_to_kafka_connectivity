package com.danskebank.kafka;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Exchanges an external (AWS STS) JWT for an Entra ID access token using the client_credentials grant
 * with a federated client assertion. Entra validates the assertion against the federated identity
 * credential (issuer, subject, audience) configured on the app registration.
 */
final class EntraFederatedTokenClient {
    static final String ASSERTION_TYPE = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";
    // Exact code only: AADSTS700213 (subject mismatch) must not match.
    private static final Pattern PROPAGATION_DELAY = Pattern.compile("AADSTS70021(?!\\d)");

    private final String tokenEndpoint;
    private final String clientId;
    private final String scope;
    private final Http http;
    private final Retry retry;

    EntraFederatedTokenClient(String tokenEndpoint, String clientId, String scope, Http http, Retry retry) {
        this.tokenEndpoint = tokenEndpoint;
        this.clientId = clientId;
        this.scope = scope;
        this.http = http;
        this.retry = retry;
    }

    String exchange(String externalJwt) throws IOException {
        String body = Http.form(
                "grant_type", "client_credentials",
                "client_id", clientId,
                "scope", scope,
                "client_assertion_type", ASSERTION_TYPE,
                "client_assertion", externalJwt);
        return retry.run("Entra ID token exchange", () -> {
            Http.Response r = http.request("POST", tokenEndpoint,
                    Map.of("Content-Type", "application/x-www-form-urlencoded"), body);
            if (r.status == 200) {
                String token = Json.string(r.body, "access_token");
                if (token == null) throw new IOException("No access_token in Entra token response");
                return token;
            }
            String error = Json.string(r.body, "error");
            String desc = Json.string(r.body, "error_description");
            String detail = "Entra ID token request failed: HTTP " + r.status
                    + (error == null ? "" : " " + error)
                    + (desc == null ? "" : ": " + firstLine(desc));
            // AADSTS70021: federated credential not yet propagated (or no match). Retry, it may resolve.
            boolean retryable = r.status >= 500 || r.status == 429
                    || (desc != null && PROPAGATION_DELAY.matcher(desc).find());
            throw retryable ? new Retry.RetryableException(detail) : new IOException(detail);
        });
    }

    private static String firstLine(String s) {
        int i = s.indexOf('\n');
        return (i < 0 ? s : s.substring(0, i)).trim();
    }
}
