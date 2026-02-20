package com.slf4u0.devicecollectorservice.exception;

public class NotRetryableException extends RuntimeException {
    public NotRetryableException(String message) {
        super(message);
    }

    public NotRetryableException(Throwable cause) {
        super(cause);
    }

    public NotRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
