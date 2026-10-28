package com.example.kafka.entra.relay;

// DATABRICKS-SHADED Kafka interfaces (the shaded client rejects vanilla org.apache.kafka.* handlers).
import kafkashaded.org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import kafkashaded.org.apache.kafka.common.security.auth.SaslExtensions;
import kafkashaded.org.apache.kafka.common.security.auth.SaslExtensionsCallback;
import kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Token relay OAUTHBEARER callback handler for the Databricks-SHADED Kafka client.
 *
 * The executors can't read a UC service credential (the documented APIs are driver and Python UDF only),
 * so the driver mints the Entra token (service credential -> STS GetWebIdentityToken -> Entra) and keeps
 * a fresh one in a file on a UC volume. On every Kafka login / re-login this handler reads that file and
 * hands the token plus the Confluent SASL extensions to the Kafka client.
 *
 * The file holds a short lived bearer token (about one hour): protect the volume with UC grants.
 * No secret is configured; clientSecret, password and username are rejected.
 *
 * Note: on UC compute the JVM can't read /Volumes paths, so token.file only works with a path every
 * executor can read. token.value (the token inline in the JAAS config) avoids the file entirely; the
 * token can't change during a query, so streams must be restarted with a fresh token before it expires.
 *
 * JAAS options (exactly one of token.value / token.file):
 *   token.value                 the Entra access token itself (set by the driver when starting the query)
 *   token.file                  a file every executor can read
 *   token.minRemainingSeconds   optional, default 300: wait for a fresher token if less is left
 *   token.maxWaitSeconds        optional, default 120: how long to wait for a fresh token
 *   extension_logicalCluster    required by Confluent Cloud (lkc-xxxxx)
 *   extension_identityPoolId    optional
 */
public class FileTokenOAuthBearerCallbackHandler implements AuthenticateCallbackHandler {

    private static final Pattern NUMBER_CLAIM = Pattern.compile("\"(exp|iat)\"\\s*:\\s*(\\d+)");
    private static final Pattern SUB_CLAIM = Pattern.compile("\"sub\"\\s*:\\s*\"([^\"]*)\"");

    private Map<String, String> options = Collections.emptyMap();
    private Path tokenFile;
    private String tokenValue;
    private long minRemainingMs;
    private long maxWaitMs;

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
            opts.put(e.getKey(), String.valueOf(e.getValue()).trim());
        }
        configure(opts);
    }

    /** Visible for tests: configure from a plain option map. */
    void configure(Map<String, String> opts) {
        for (String forbidden : new String[]{"clientSecret", "password", "username"}) {
            if (present(opts.get(forbidden))) {
                throw new IllegalArgumentException("JAAS option '" + forbidden + "' is not allowed");
            }
        }
        this.options = opts;
        String file = opts.get("token.file");
        String value = opts.get("token.value");
        if (present(file) == present(value)) {
            throw new IllegalArgumentException("Set exactly one JAAS option: token.value or token.file");
        }
        this.tokenFile = present(file) ? Paths.get(file) : null;
        this.tokenValue = present(value) ? value : null;
        this.minRemainingMs = longOpt("token.minRemainingSeconds", 300) * 1000L;
        this.maxWaitMs = longOpt("token.maxWaitSeconds", 120) * 1000L;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback cb : callbacks) {
            if (cb instanceof OAuthBearerTokenCallback) {
                OAuthBearerTokenCallback tc = (OAuthBearerTokenCallback) cb;
                try {
                    tc.token(readToken());
                } catch (Exception ex) {
                    tc.error("token_relay_failed", ex.getMessage(), null);
                }
            } else if (cb instanceof SaslExtensionsCallback) {
                Map<String, String> ext = new LinkedHashMap<>();
                putIfPresent(ext, "logicalCluster", options.get("extension_logicalCluster"));
                putIfPresent(ext, "identityPoolId", options.get("extension_identityPoolId"));
                ((SaslExtensionsCallback) cb).extensions(new SaslExtensions(ext));
            } else {
                throw new UnsupportedCallbackException(cb);
            }
        }
    }

    /** Reads the relayed token; waits (polling) while the file is missing, partial or close to expiry. */
    OAuthBearerToken readToken() throws IOException, InterruptedException {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        String problem;
        while (true) {
            problem = null;
            String jwt = null;
            if (tokenValue != null) {
                jwt = tokenValue;
            } else {
                try {
                    jwt = new String(Files.readAllBytes(tokenFile), StandardCharsets.UTF_8).trim();
                } catch (IOException e) {
                    problem = "cannot read " + tokenFile + " (" + e.getClass().getSimpleName() + ")";
                }
            }
            if (jwt != null) {
                String payload = payload(jwt);
                Long exp = numberClaim(payload, "exp");
                if (payload == null || exp == null) {
                    problem = "token file does not contain a JWT with exp";
                } else if (exp * 1000L - System.currentTimeMillis() < minRemainingMs) {
                    problem = tokenValue != null
                            ? "inline token expires too soon: restart the query with a fresh token"
                            : "relayed token expires too soon (is the driver refresher running?)";
                } else {
                    Long iat = numberClaim(payload, "iat");
                    Matcher sub = SUB_CLAIM.matcher(payload);
                    return new SimpleToken(jwt, sub.find() ? sub.group(1) : "unknown", exp * 1000L,
                            iat == null ? System.currentTimeMillis() : iat * 1000L);
                }
            }
            if (tokenValue != null || System.currentTimeMillis() >= deadline) {
                throw new IOException("Token relay failed: " + problem);
            }
            Thread.sleep(2_000L);
        }
    }

    @Override
    public void close() { /* no-op */ }

    private static String payload(String jwt) {
        String[] parts = jwt.split("\\.");
        if (parts.length != 3) return null;
        try {
            String p = parts[1];
            int m = p.length() % 4;
            if (m != 0) p = p + "====".substring(m);
            return new String(Base64.getUrlDecoder().decode(p), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static Long numberClaim(String payload, String name) {
        if (payload == null) return null;
        Matcher m = NUMBER_CLAIM.matcher(payload);
        while (m.find()) {
            if (m.group(1).equals(name)) return Long.parseLong(m.group(2));
        }
        return null;
    }

    private long longOpt(String key, long def) {
        String v = options.get(key);
        if (!present(v)) return def;
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("JAAS option " + key + " must be an integer");
        }
    }

    private static boolean present(String v) {
        return v != null && !v.isEmpty() && !"null".equals(v);
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
            this.principal = principal;
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
