package com.danskebank.kafka;

import java.io.IOException;

/** Retries only RetryableException, with exponential backoff. */
final class Retry {
    interface Call<T> {
        T run() throws IOException;
    }

    static final class RetryableException extends IOException {
        RetryableException(String message) {
            super(message);
        }
    }

    private final int maxAttempts;
    private final long initialBackoffMs;

    Retry(int maxAttempts, long initialBackoffMs) {
        this.maxAttempts = Math.max(1, maxAttempts);
        this.initialBackoffMs = Math.max(0, initialBackoffMs);
    }

    <T> T run(String what, Call<T> call) throws IOException {
        long backoff = initialBackoffMs;
        for (int attempt = 1; ; attempt++) {
            try {
                return call.run();
            } catch (RetryableException e) {
                if (attempt >= maxAttempts) {
                    throw new IOException(e.getMessage() + " (after " + attempt + " attempts)");
                }
            } catch (java.net.SocketTimeoutException | java.net.ConnectException e) {
                if (attempt >= maxAttempts) {
                    throw new IOException(what + " failed: " + e.getMessage() + " (after " + attempt + " attempts)", e);
                }
            }
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException(what + " interrupted", ie);
            }
            backoff *= 2;
        }
    }
}
