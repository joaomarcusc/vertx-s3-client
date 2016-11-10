package com.hubrick.vertx.s3.client;

import com.google.common.base.Strings;
import com.hubrick.vertx.s3.S3Headers;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;

import java.text.MessageFormat;
import java.time.Clock;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class S3Client {

    public static final String DEFAULT_REGION = "us-east-1";
    public static final String DEFAULT_ENDPOINT = "s3.amazonaws.com";
    private static final String ENDPOINT_PATTERN = "s3-{0}.amazonaws.com";

    private final Long globalTimeout;
    private final String awsRegion;

    private final String hostname;

    private final Clock clock;
    private final HttpClient client;
    private final String awsAccessKey;
    private final String awsSecretKey;
    private final String awsServiceName;

    public S3Client(Vertx vertx,
                    S3ClientOptions s3ClientOptions) {
        this(vertx, s3ClientOptions, null, Clock.systemDefaultZone());
    }

    public S3Client(Vertx vertx,
                    S3ClientOptions s3ClientOptions,
                    String hostnameOverride,
                    Clock clock) {
        checkNotNull(vertx, "vertx must not be null");
        checkNotNull(isNotBlank(s3ClientOptions.getAwsRegion()), "AWS region must be set");
        checkNotNull(isNotBlank(s3ClientOptions.getAwsServiceName()), "AWS service name must be set");
        checkNotNull(clock, "Clock must not be null");
        checkNotNull(s3ClientOptions.getGlobalTimeoutMs(), "global timeout must be null");
        checkArgument(s3ClientOptions.getGlobalTimeoutMs() > 0, "global timeout must be more than zero ms");


        this.clock = clock;
        this.awsServiceName = s3ClientOptions.getAwsServiceName();
        this.awsRegion = s3ClientOptions.getAwsRegion();
        this.awsAccessKey = s3ClientOptions.getAwsAccessKey();
        this.awsSecretKey = s3ClientOptions.getAwsSecretKey();
        this.globalTimeout = s3ClientOptions.getGlobalTimeoutMs();


        if (!Strings.isNullOrEmpty(hostnameOverride)) {
            hostname = hostnameOverride;
        } else {
            if (DEFAULT_REGION.equals(s3ClientOptions.getAwsRegion())) {
                hostname = DEFAULT_ENDPOINT;
            } else {
                hostname = MessageFormat.format(ENDPOINT_PATTERN, awsRegion);
            }
        }

        this.client = vertx.createHttpClient(s3ClientOptions);
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public String getAwsServiceName() {
        return awsServiceName;
    }

    public String getHostname() {
        return hostname;
    }

    public void close() {
        client.close();
    }

    public Long getGlobalTimeout() {
        return globalTimeout;
    }

    // Direct call (async)
    // -----------

    // GET (bucket, key) -> handler(Data)
    public void get(String bucket,
                    String key,
                    Handler<HttpClientResponse> handler,
                    Handler<Throwable> exceptionHandler) {
        S3ClientRequest request = createGetRequest(bucket, key, handler);
        request.exceptionHandler(exceptionHandler);
        request.end();
    }

    // PUT (bucket, key, data) -> handler(Response)
    public void put(String bucket,
                    String key,
                    Buffer data,
                    Handler<HttpClientResponse> handler,
                    Handler<Throwable> exceptionHandler) {
        S3ClientRequest request = createPutRequest(bucket, key, handler);
        request.exceptionHandler(exceptionHandler);
        request.end(data);
    }

    // PUT (bucket, key, data) -> handler(Response)
    public void copy(String sourceBucket,
                     String sourceKey,
                     String destinationBucket,
                     String destinationKey,
                     Handler<HttpClientResponse> handler,
                     Handler<Throwable> exceptionHandler) {
        S3ClientRequest request = createCopyRequest(sourceBucket, sourceKey, destinationBucket, destinationKey, handler);
        request.exceptionHandler(exceptionHandler);
        request.end();
    }

    // DELETE (bucket, key) -> handler(Response)
    public void delete(String bucket,
                       String key,
                       Handler<HttpClientResponse> handler,
                       Handler<Throwable> exceptionHandler) {
        S3ClientRequest request = createDeleteRequest(bucket, key, handler);
        request.exceptionHandler(exceptionHandler);
        request.end();
    }

    // Create requests which can be customized
    // ---------------------------------------

    // create PUT -> requestObject (which you can do stuff with)
    public S3ClientRequest createPutRequest(String bucket,
                                            String key,
                                            Handler<HttpClientResponse> handler) {
        HttpClientRequest httpRequest = client.put("/" + bucket + "/" + key,
                handler);
        return new S3ClientRequest("PUT",
                awsRegion,
                awsServiceName,
                bucket,
                key,
                httpRequest,
                awsAccessKey,
                awsSecretKey,
                clock)
                .setTimeout(globalTimeout)
                .putHeader("Host", hostname);

    }

    // create PUT -> requestObject (which you can do stuff with)
    public S3ClientRequest createCopyRequest(String sourceBucket,
                                             String sourceKey,
                                             String destinationBucket,
                                             String destinationKey,
                                             Handler<HttpClientResponse> handler) {
        final HttpClientRequest httpRequest = client.put(
                "/" + destinationBucket + "/" + destinationKey,
                handler
        );

        final S3ClientRequest s3ClientRequest = new S3ClientRequest("PUT",
                awsRegion,
                awsServiceName,
                destinationBucket,
                destinationKey,
                httpRequest,
                awsAccessKey,
                awsSecretKey,
                clock
        ).setTimeout(globalTimeout)
         .putHeader("Host", hostname);

        return s3ClientRequest.putHeader(S3Headers.COPY_SOURCE_HEADER.getValue(), "/" + sourceBucket + "/" + sourceKey);
    }

    // create GET -> request Object
    public S3ClientRequest createGetRequest(String bucket,
                                            String key,
                                            Handler<HttpClientResponse> handler) {
        HttpClientRequest httpRequest = client.get("/" + bucket + "/" + key,
                handler);
        return new S3ClientRequest("GET",
                awsRegion,
                awsServiceName,
                bucket,
                key,
                httpRequest,
                awsAccessKey,
                awsSecretKey,
                clock)
                .setTimeout(globalTimeout)
                .putHeader("Host", hostname);
    }

    // create DELETE -> request Object
    public S3ClientRequest createDeleteRequest(String bucket,
                                               String key,
                                               Handler<HttpClientResponse> handler) {
        HttpClientRequest httpRequest = client.delete("/" + bucket + "/" + key,
                handler);
        return new S3ClientRequest("DELETE",
                awsRegion,
                awsServiceName,
                bucket,
                key,
                httpRequest,
                awsAccessKey,
                awsSecretKey,
                clock)
                .setTimeout(globalTimeout)
                .putHeader("Host", hostname);

    }
}
