package com.hubrick.vertx.s3.signature;

/**
 * @author marcus
 * @since 1.0.0
 */
public class AWS4SignatureException extends RuntimeException {

    public AWS4SignatureException(final String message) {
        super(message);
    }

}
