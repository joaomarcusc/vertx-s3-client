package com.hubrick.vertx.s3.client;

import com.google.common.base.Strings;
import com.hubrick.vertx.s3.S3Headers;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;

import java.text.MessageFormat;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class S3Client {

    public static final String DEFAULT_REGION = "us-east-1";
    public static final String DEFAULT_ENDPOINT = "s3.amazonaws.com";
    private static final String ENDPOINT_PATTERN = "s3-{0}.amazonaws.com";

    private final String awsAccessKey;
    private final String awsSecretKey;
    private final String region;
    private final Integer port;
    private final String serviceName;
    private final String hostname;

    private Long globalTimeout = 10000L;

    private final HttpClient client;

    public S3Client(Vertx vertx,
                    HttpClientOptions httpClientOptions,
                    String region,
                    Integer port,
                    String serviceName,
                    String accessKey,
                    String secretKey) {
        this(vertx, httpClientOptions, region, port, serviceName, accessKey, secretKey, null);
    }

    public S3Client(Vertx vertx,
                    HttpClientOptions httpClientOptions,
                    String region,
                    Integer port,
                    String serviceName,
                    String accessKey,
                    String secretKey,
                    String hostnameOverride) {
        checkNotNull(vertx, "vertx must not be null");
        checkNotNull(region, "region must not be null");
        checkArgument(!region.isEmpty(), "region must not be empty");
        checkNotNull(port, "port must not be null");
        checkNotNull(serviceName, "serviceName must not be null");
        checkArgument(!serviceName.isEmpty(), "serviceName must not be empty");
        checkNotNull(accessKey, "accessKey must not be null");
        checkArgument(!accessKey.isEmpty(), "accessKey must not be empty");
        checkNotNull(secretKey, "secretKey must not be null");
        checkArgument(!secretKey.isEmpty(), "secretKey must not be empty");

        this.region = region;
        this.port = port;
        this.serviceName = serviceName;
        this.awsAccessKey = accessKey;
        this.awsSecretKey = secretKey;

        if (!Strings.isNullOrEmpty(hostnameOverride)) {
            hostname = hostnameOverride;
        } else {
            if (DEFAULT_REGION.equals(region.toLowerCase())) {
                hostname = DEFAULT_ENDPOINT;
            } else {
                hostname = MessageFormat.format(ENDPOINT_PATTERN, region);
            }
        }

        httpClientOptions.setDefaultHost(hostname);
        httpClientOptions.setDefaultPort(port);
        this.client = vertx.createHttpClient(httpClientOptions);
    }

    public String getRegion() {
        return region;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getHostname() {
        return hostname;
    }

    public Integer getPort() {
        return port;
    }

    public void close() {
        client.close();
    }

    public Long getGlobalTimeout() {
        return globalTimeout;
    }

    public void setGlobalTimeout(Long globalTimeout) {
        this.globalTimeout = globalTimeout;
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
                region,
                serviceName,
                bucket,
                key,
                httpRequest,
                awsAccessKey,
                awsSecretKey)
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
                region,
                serviceName,
                destinationBucket,
                destinationKey,
                httpRequest,
                awsAccessKey,
                awsSecretKey
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
                region,
                serviceName,
                bucket,
                key,
                httpRequest,
                awsAccessKey,
                awsSecretKey)
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
                region,
                serviceName,
                bucket,
                key,
                httpRequest,
                awsAccessKey,
                awsSecretKey)
                .setTimeout(globalTimeout)
                .putHeader("Host", hostname);

    }
}
