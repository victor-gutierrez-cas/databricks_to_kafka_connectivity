package com.danskebank.kafka;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Minimal AWS Signature Version 4 signer for a form-encoded POST to the root path ("/"),
 * which is all the STS query API needs. Dependency free (JDK only).
 */
final class SigV4 {
    private static final DateTimeFormatter AMZ_DATE = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");
    static final String CONTENT_TYPE = "application/x-www-form-urlencoded; charset=utf-8";

    private SigV4() {}

    /** Returns the headers to send (Content-Type, X-Amz-Date, optional X-Amz-Security-Token, Authorization). */
    static Map<String, String> signFormPost(String url, String body, String region, String service,
                                            AwsCredentials creds, ZonedDateTime now) {
        URI uri = URI.create(url);
        String host = uri.getPort() == -1 ? uri.getHost() : uri.getHost() + ":" + uri.getPort();
        String path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
        String amzDate = now.withZoneSameInstant(ZoneOffset.UTC).format(AMZ_DATE);
        String date = amzDate.substring(0, 8);

        TreeMap<String, String> signed = new TreeMap<>();
        signed.put("content-type", CONTENT_TYPE);
        signed.put("host", host);
        signed.put("x-amz-date", amzDate);
        if (creds.sessionToken != null) signed.put("x-amz-security-token", creds.sessionToken);

        StringBuilder canonicalHeaders = new StringBuilder();
        for (Map.Entry<String, String> e : signed.entrySet()) {
            canonicalHeaders.append(e.getKey()).append(':').append(e.getValue().trim()).append('\n');
        }
        String signedHeaders = String.join(";", signed.keySet());

        String canonicalRequest = "POST\n" + path + "\n" + "\n" + canonicalHeaders + "\n" + signedHeaders + "\n"
                + hex(sha256(body.getBytes(StandardCharsets.UTF_8)));
        String scope = date + "/" + region + "/" + service + "/aws4_request";
        String stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n"
                + hex(sha256(canonicalRequest.getBytes(StandardCharsets.UTF_8)));

        byte[] kDate = hmac(("AWS4" + creds.secretAccessKey).getBytes(StandardCharsets.UTF_8), date);
        byte[] kRegion = hmac(kDate, region);
        byte[] kService = hmac(kRegion, service);
        byte[] kSigning = hmac(kService, "aws4_request");
        String signature = hex(hmac(kSigning, stringToSign));

        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("Content-Type", CONTENT_TYPE);
        headers.put("X-Amz-Date", amzDate);
        if (creds.sessionToken != null) headers.put("X-Amz-Security-Token", creds.sessionToken);
        headers.put("Authorization", "AWS4-HMAC-SHA256 Credential=" + creds.accessKeyId + "/" + scope
                + ", SignedHeaders=" + signedHeaders + ", Signature=" + signature);
        return headers;
    }

    private static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] hmac(byte[] key, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
