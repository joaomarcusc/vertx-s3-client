package com.hubrick.vertx.s3.client;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.Http2Settings;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpVersion;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JdkSSLEngineOptions;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.KeyCertOptions;
import io.vertx.core.net.OpenSSLEngineOptions;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.core.net.PfxOptions;
import io.vertx.core.net.ProxyOptions;
import io.vertx.core.net.SSLEngineOptions;
import io.vertx.core.net.TrustOptions;

import java.util.List;

/**
 * @author marcus
 * @since 1.0.0
 */
public class S3ClientOptions extends HttpClientOptions {

    private String awsAccessKey;
    private String awsSecretKey;
    private String awsRegion;
    private String awsServiceName;
    private Long globalTimeoutMs = 10000L;


    public S3ClientOptions() {
        super();
    }

    public S3ClientOptions(final S3ClientOptions other) {
        super(other);

        setAwsAccessKey(other.getAwsAccessKey());
        setAwsSecretKey(other.getAwsSecretKey());
        setAwsRegion(other.getAwsRegion());
        setAwsServiceName(other.getAwsServiceName());
        setGlobalTimeoutMs(other.getGlobalTimeoutMs());
    }

    public S3ClientOptions(final HttpClientOptions other) {
        super(other);
    }

    public S3ClientOptions(final JsonObject json) {
        super(json);

        setAwsAccessKey(json.getString("awsAccessKey"));
        setAwsSecretKey(json.getString("awsSecretKey"));
        setAwsRegion(json.getString("awsRegion"));
        setAwsServiceName(json.getString("awsServiceName"));
        setGlobalTimeoutMs(json.getLong("globalTimeoutMs"));
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public S3ClientOptions setAwsAccessKey(final String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
        return this;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public S3ClientOptions setAwsSecretKey(final String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
        return this;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public S3ClientOptions setAwsRegion(final String awsRegion) {
        this.awsRegion = awsRegion;
        return this;
    }

    public String getAwsServiceName() {
        return awsServiceName;
    }

    public S3ClientOptions setAwsServiceName(final String awsServiceName) {
        this.awsServiceName = awsServiceName;
        return this;
    }

    public Long getGlobalTimeoutMs() {
        return globalTimeoutMs;
    }

    public S3ClientOptions setGlobalTimeoutMs(final Long globalTimeoutMs) {
        this.globalTimeoutMs = globalTimeoutMs;
        return this;
    }

    @Override
    public S3ClientOptions setSendBufferSize(final int sendBufferSize) {
        super.setSendBufferSize(sendBufferSize);
        return this;
    }

    @Override
    public S3ClientOptions setReceiveBufferSize(final int receiveBufferSize) {
        super.setReceiveBufferSize(receiveBufferSize);
        return this;
    }

    @Override
    public S3ClientOptions setReuseAddress(final boolean reuseAddress) {
        super.setReuseAddress(reuseAddress);
        return this;
    }

    @Override
    public S3ClientOptions setTrafficClass(final int trafficClass) {
        super.setTrafficClass(trafficClass);
        return this;
    }

    @Override
    public S3ClientOptions setTcpNoDelay(final boolean tcpNoDelay) {
        super.setTcpNoDelay(tcpNoDelay);
        return this;
    }

    @Override
    public S3ClientOptions setTcpKeepAlive(final boolean tcpKeepAlive) {
        super.setTcpKeepAlive(tcpKeepAlive);
        return this;
    }

    @Override
    public S3ClientOptions setSoLinger(final int soLinger) {
        super.setSoLinger(soLinger);
        return this;
    }

    @Override
    public S3ClientOptions setUsePooledBuffers(final boolean usePooledBuffers) {
        super.setUsePooledBuffers(usePooledBuffers);
        return this;
    }

    @Override
    public S3ClientOptions setIdleTimeout(final int idleTimeout) {
        super.setIdleTimeout(idleTimeout);
        return this;
    }

    @Override
    public S3ClientOptions setSsl(final boolean ssl) {
        super.setSsl(ssl);
        return this;
    }

    @Override
    public S3ClientOptions setKeyCertOptions(final KeyCertOptions options) {
        super.setKeyCertOptions(options);
        return this;
    }

    @Override
    public S3ClientOptions setKeyStoreOptions(final JksOptions options) {
        super.setKeyStoreOptions(options);
        return this;
    }

    @Override
    public S3ClientOptions setPfxKeyCertOptions(final PfxOptions options) {
        super.setPfxKeyCertOptions(options);
        return this;
    }

    @Override
    public S3ClientOptions setTrustOptions(final TrustOptions options) {
        super.setTrustOptions(options);
        return this;
    }

    @Override
    public S3ClientOptions setPemKeyCertOptions(final PemKeyCertOptions options) {
        super.setPemKeyCertOptions(options);
        return this;
    }

    @Override
    public S3ClientOptions setTrustStoreOptions(final JksOptions options) {
        super.setTrustStoreOptions(options);
        return this;
    }

    @Override
    public S3ClientOptions setPfxTrustOptions(final PfxOptions options) {
        super.setPfxTrustOptions(options);
        return this;
    }

    @Override
    public S3ClientOptions setPemTrustOptions(final PemTrustOptions options) {
        super.setPemTrustOptions(options);
        return this;
    }

    @Override
    public S3ClientOptions addEnabledCipherSuite(final String suite) {
        super.addEnabledCipherSuite(suite);
        return this;
    }

    @Override
    public S3ClientOptions addEnabledSecureTransportProtocol(final String protocol) {
        super.addEnabledSecureTransportProtocol(protocol);
        return this;
    }

    @Override
    public S3ClientOptions addCrlPath(final String crlPath) throws NullPointerException {
        super.addCrlPath(crlPath);
        return this;
    }

    @Override
    public S3ClientOptions addCrlValue(final Buffer crlValue) throws NullPointerException {
        super.addCrlValue(crlValue);
        return this;
    }

    @Override
    public S3ClientOptions setConnectTimeout(final int connectTimeout) {
        super.setConnectTimeout(connectTimeout);
        return this;
    }

    @Override
    public S3ClientOptions setTrustAll(final boolean trustAll) {
        super.setTrustAll(trustAll);
        return this;
    }

    @Override
    public S3ClientOptions setHttp2MultiplexingLimit(final int limit) {
        super.setHttp2MultiplexingLimit(limit);
        return this;
    }

    @Override
    public S3ClientOptions setMaxPoolSize(final int maxPoolSize) {
        super.setMaxPoolSize(maxPoolSize);
        return this;
    }

    @Override
    public S3ClientOptions setHttp2MaxPoolSize(final int max) {
        super.setHttp2MaxPoolSize(max);
        return this;
    }

    @Override
    public S3ClientOptions setHttp2ConnectionWindowSize(final int http2ConnectionWindowSize) {
        super.setHttp2ConnectionWindowSize(http2ConnectionWindowSize);
        return this;
    }

    @Override
    public S3ClientOptions setKeepAlive(final boolean keepAlive) {
        super.setKeepAlive(keepAlive);
        return this;
    }

    @Override
    public S3ClientOptions setPipelining(final boolean pipelining) {
        super.setPipelining(pipelining);
        return this;
    }

    @Override
    public S3ClientOptions setPipeliningLimit(final int limit) {
        super.setPipeliningLimit(limit);
        return this;
    }

    @Override
    public S3ClientOptions setVerifyHost(final boolean verifyHost) {
        super.setVerifyHost(verifyHost);
        return this;
    }

    @Override
    public S3ClientOptions setTryUseCompression(final boolean tryUseCompression) {
        super.setTryUseCompression(tryUseCompression);
        return this;
    }

    @Override
    public S3ClientOptions setMaxWebsocketFrameSize(final int maxWebsocketFrameSize) {
        super.setMaxWebsocketFrameSize(maxWebsocketFrameSize);
        return this;
    }

    @Override
    public S3ClientOptions setDefaultHost(final String defaultHost) {
        super.setDefaultHost(defaultHost);
        return this;
    }

    @Override
    public S3ClientOptions setDefaultPort(final int defaultPort) {
        super.setDefaultPort(defaultPort);
        return this;
    }

    @Override
    public S3ClientOptions setProtocolVersion(final HttpVersion protocolVersion) {
        super.setProtocolVersion(protocolVersion);
        return this;
    }

    @Override
    public S3ClientOptions setMaxChunkSize(final int maxChunkSize) {
        super.setMaxChunkSize(maxChunkSize);
        return this;
    }

    @Override
    public S3ClientOptions setMaxWaitQueueSize(final int maxWaitQueueSize) {
        super.setMaxWaitQueueSize(maxWaitQueueSize);
        return this;
    }

    @Override
    public S3ClientOptions setInitialSettings(final Http2Settings settings) {
        super.setInitialSettings(settings);
        return this;
    }

    @Override
    public S3ClientOptions setSslEngineOptions(final SSLEngineOptions sslEngineOptions) {
        super.setSslEngineOptions(sslEngineOptions);
        return this;
    }

    @Override
    public S3ClientOptions setJdkSslEngineOptions(final JdkSSLEngineOptions sslEngineOptions) {
        super.setJdkSslEngineOptions(sslEngineOptions);
        return this;
    }

    @Override
    public S3ClientOptions setOpenSslEngineOptions(final OpenSSLEngineOptions sslEngineOptions) {
        super.setOpenSslEngineOptions(sslEngineOptions);
        return this;
    }

    @Override
    public S3ClientOptions setAlpnVersions(final List<HttpVersion> alpnVersions) {
        super.setAlpnVersions(alpnVersions);
        return this;
    }

    @Override
    public S3ClientOptions setHttp2ClearTextUpgrade(final boolean value) {
        super.setHttp2ClearTextUpgrade(value);
        return this;
    }

    @Override
    public S3ClientOptions setMetricsName(final String metricsName) {
        super.setMetricsName(metricsName);
        return this;
    }

    @Override
    public S3ClientOptions setProxyOptions(final ProxyOptions proxyOptions) {
        super.setProxyOptions(proxyOptions);
        return this;
    }

    @Override
    public S3ClientOptions setLogActivity(final boolean logEnabled) {
        super.setLogActivity(logEnabled);
        return this;
    }
}
