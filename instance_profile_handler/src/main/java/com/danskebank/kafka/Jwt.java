package com.danskebank.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Reads claims from a JWT payload without verifying it (Entra and Confluent do the verification). */
final class Jwt {
    private Jwt() {}

    static String payload(String jwt) {
        String[] parts = jwt.split("\\.");
        if (parts.length < 2) throw new IllegalArgumentException("Not a JWT");
        return new String(Base64.getUrlDecoder().decode(pad(parts[1])), StandardCharsets.UTF_8);
    }

    static String claim(String jwt, String claim) {
        try {
            return Json.string(payload(jwt), claim);
        } catch (Exception e) {
            return null;
        }
    }

    /** Returns [expiryMs, issuedAtMs]. Falls back to now + 50 minutes if the claims cannot be read. */
    static long[] expiryAndIssuedAtMs(String jwt) {
        long now = System.currentTimeMillis();
        try {
            String p = payload(jwt);
            Long exp = Json.number(p, "exp");
            Long iat = Json.number(p, "iat");
            if (exp == null) throw new IllegalArgumentException("no exp");
            return new long[]{exp * 1000L, iat == null ? now : iat * 1000L};
        } catch (Exception e) {
            return new long[]{now + 3_000_000L, now};
        }
    }

    private static String pad(String b64) {
        int m = b64.length() % 4;
        return m == 0 ? b64 : b64 + "====".substring(m);
    }
}
