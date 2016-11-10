package com.hubrick.vertx.s3.client;

import com.hubrick.vertx.s3.S3Headers;
import com.hubrick.vertx.s3.signature.AWS4SignatureBuilder;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpConnection;
import io.vertx.core.http.HttpFrame;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.Map;

public class S3ClientRequest implements HttpClientRequest {

    private static final Logger log = LoggerFactory.getLogger(S3ClientRequest.class);

    private final HttpClientRequest request;

    // These are actually set when the request is created, but we need to know
    private final String method;
    private final String region;
    private final String serviceName;
    private final String bucket;
    private final String key;

    // Used for authentication(which may be optional depending on the bucket)
    private String awsAccessKey;
    private String awsSecretKey;

    public S3ClientRequest(String method,
                           String region,
                           String serviceName,
                           String bucket,
                           String key,
                           HttpClientRequest request) {
        this(method, region, serviceName, bucket, key, request, null, null);
    }

    public S3ClientRequest(String method,
                           String region,
                           String serviceName,
                           String bucket,
                           String key,
                           HttpClientRequest request,
                           String awsAccessKey,
                           String awsSecretKey) {
        this.method = method;
        this.region = region;
        this.serviceName = serviceName;
        this.bucket = bucket;
        this.key = key;
        this.request = request;
        this.awsAccessKey = awsAccessKey;
        this.awsSecretKey = awsSecretKey;
    }

    @Override
    public S3ClientRequest setWriteQueueMaxSize(int maxSize) {
        request.setWriteQueueMaxSize(maxSize);
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return request.writeQueueFull();
    }

    @Override
    public S3ClientRequest drainHandler(Handler<Void> handler) {
        request.drainHandler(handler);
        return this;
    }

    @Override
    public S3ClientRequest handler(Handler<HttpClientResponse> handler) {
        request.handler(handler);
        return this;
    }

    @Override
    public S3ClientRequest pause() {
        request.pause();
        return this;
    }

    @Override
    public S3ClientRequest resume() {
        request.resume();
        return this;
    }

    @Override
    public S3ClientRequest endHandler(Handler<Void> endHandler) {
        request.endHandler(endHandler);
        return this;
    }

    @Override
    public S3ClientRequest exceptionHandler(Handler<Throwable> handler) {
        request.exceptionHandler(handler);
        return this;
    }

    @Override
    public S3ClientRequest setChunked(boolean chunked) {
        request.setChunked(chunked);
        return this;
    }

    @Override
    public boolean isChunked() {
        return request.isChunked();
    }

    @Override
    public HttpMethod method() {
        return null;
    }

    @Override
    public String getRawMethod() {
        return request.getRawMethod();
    }

    @Override
    public S3ClientRequest setRawMethod(String s) {
        request.setRawMethod(s);
        return this;
    }

    @Override
    public String uri() {
        return request.uri();
    }

    @Override
    public String path() {
        return request.path();
    }

    @Override
    public String query() {
        return request.query();
    }

    @Override
    public S3ClientRequest setHost(String s) {
        request.setHost(s);
        return this;
    }

    @Override
    public String getHost() {
        return request.getHost();
    }

    @Override
    public MultiMap headers() {
        return request.headers();
    }

    @Override
    public S3ClientRequest putHeader(String name, String value) {
        request.putHeader(name, value);
        return this;
    }

    @Override
    public S3ClientRequest putHeader(CharSequence name, CharSequence value) {
        request.putHeader(name, value);
        return this;
    }

    @Override
    public S3ClientRequest putHeader(String name, Iterable<String> values) {
        request.putHeader(name, values);
        return this;
    }

    @Override
    public S3ClientRequest putHeader(CharSequence name, Iterable<CharSequence> values) {
        request.putHeader(name, values);
        return this;
    }

    @Override
    public S3ClientRequest write(Buffer chunk) {
        request.write(chunk);
        return this;
    }

    @Override
    public S3ClientRequest write(String chunk) {
        request.write(chunk);
        return this;
    }

    @Override
    public S3ClientRequest write(String chunk, String enc) {
        request.write(chunk, enc);
        return this;
    }

    @Override
    public S3ClientRequest continueHandler(Handler<Void> handler) {
        request.continueHandler(handler);
        return this;
    }

    @Override
    public S3ClientRequest sendHead() {
        // Generate authentication header
        initAuthenticationHeader();
        // Send the header
        request.sendHead();
        return this;
    }

    @Override
    public HttpClientRequest sendHead(Handler<HttpVersion> handler) {
        return request.sendHead(handler);
    }

    @Override
    public void end(String chunk) {
        // Generate authentication header
        initAuthenticationHeader();
        request.end(chunk);
    }

    @Override
    public void end(String chunk, String enc) {
        // Generate authentication header
        initAuthenticationHeader();
        request.end(chunk, enc);
    }

    @Override
    public void end(Buffer chunk) {
        // Generate authentication header
        initAuthenticationHeader();
        request.end(chunk);
    }

    @Override
    public void end() {
        // Generate authentication header
        initAuthenticationHeader();
        request.end();
    }

    @Override
    public S3ClientRequest setTimeout(long timeoutMs) {
        request.setTimeout(timeoutMs);
        return this;
    }

    @Override
    public S3ClientRequest pushHandler(Handler<HttpClientRequest> handler) {
        request.pushHandler(handler);
        return this;
    }

    @Override
    public void reset() {
        request.reset();
    }

    @Override
    public void reset(long l) {
        request.reset(l);
    }

    @Override
    public HttpConnection connection() {
        return request.connection();
    }

    @Override
    public S3ClientRequest connectionHandler(Handler<HttpConnection> handler) {
        request.connectionHandler(handler);
        return this;
    }

    @Override
    public S3ClientRequest writeCustomFrame(int i, int i1, Buffer buffer) {
        request.writeCustomFrame(i, i1, buffer);
        return this;
    }

    @Override
    public int streamId() {
        return request.streamId();
    }

    @Override
    public S3ClientRequest writeCustomFrame(HttpFrame frame) {
        request.writeCustomFrame(frame);
        return this;
    }

    protected void initAuthenticationHeader() {
        if (isAuthenticated()) {
            final String canonicalizedResource = "/" + bucket + "/" + key;

            final AWS4SignatureBuilder signatureBuilder = AWS4SignatureBuilder
                    .builder(ZonedDateTime.now(), region, serviceName)
                    .httpRequestMethod(method)
                    .canonicalUri(canonicalizedResource)
                    .awsSecretKey(awsSecretKey);

            headers().set(S3Headers.DATE.getValue(), signatureBuilder.makeSignatureFormattedDate());

            for (Map.Entry<String, String> entry : headers()) {
                signatureBuilder.header(entry.getKey(), entry.getValue());
            }

            headers().set(S3Headers.CONTENT_SHA.getValue(), AWS4SignatureBuilder.UNSIGNED_PAYLOAD);

            log.info("S3 toSign:\n{}", signatureBuilder.makeCanonicalRequest());

            try {

                headers().set("Authorization", signatureBuilder.buildAuthorizationHeaderValue(awsAccessKey));

            } catch (Exception e) {
                // This will totally fail,
                // but downstream users can handle it
                log.error("Failed to sign S3 request due to " + e.getMessage(), e);
            }

        }
        // Otherwise not needed
    }

    public boolean isAuthenticated() {
        return awsAccessKey != null && awsSecretKey != null;
    }

    public void setAwsAccessKey(String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
    }

    public void setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
    }

    public String getMethod() {
        return method;
    }

    public String getContentType() {
        return headers().get(HttpHeaders.CONTENT_TYPE);
    }

    public void setContentType(String contentType) {
        headers().set(HttpHeaders.CONTENT_TYPE, contentType);
    }

}
