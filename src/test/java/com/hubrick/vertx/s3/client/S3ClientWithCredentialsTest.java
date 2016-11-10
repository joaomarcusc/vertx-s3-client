package com.hubrick.vertx.s3.client;

import com.hubrick.vertx.s3.AbstractFunctionalTest;
import com.hubrick.vertx.s3.S3TestCredentials;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.model.Header;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static com.hubrick.vertx.s3.VertxMatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * @author marcus
 * @since 1.0.0
 */
public class S3ClientWithCredentialsTest extends AbstractFunctionalTest {

    public static final String HOSTNAME = "localhost";
    private S3Client s3Client;

    @Before
    public void setUp() throws Exception {
        final S3ClientOptions clientOptions = new S3ClientOptions();
        clientOptions.setDefaultHost(HOSTNAME);
        clientOptions.setDefaultPort(MOCKSERVER_PORT);
        clientOptions.setMaxPoolSize(10);
        clientOptions.setAwsRegion(S3TestCredentials.REGION);
        clientOptions.setAwsServiceName(S3TestCredentials.SERVICE_NAME);
        clientOptions.setAwsAccessKey(S3TestCredentials.AWS_ACCESS_KEY);
        clientOptions.setAwsSecretKey(S3TestCredentials.AWS_SECRET_KEY);

        s3Client = new S3Client(
                vertx,
                clientOptions,
                HOSTNAME,
                Clock.fixed(Instant.ofEpochSecond(1478782934), ZoneId.of("UTC")));

    }

    @Test
    public void testGet(TestContext testContext) {
        getMockServerClient().when(
                request()
                        .withMethod("GET")
                        .withPath("/bucket/key")
                        .withHeader("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
                        .withHeader("X-Amz-Date", "20161110T130214Z")
                        .withHeader("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=77f53b6d1eb26273da59491e98ea10989a006a57620d7351be44b596a348d699")

        ).respond(
                response()
                        .withStatusCode(200)
                        .withHeader(Header.header("Content-Type", "application/json;charset=UTF-8"))
                        .withBody("response")
        );


        final Async async = testContext.async();
        s3Client.get("bucket", "key",
                (response) -> {
                    assertThat(testContext, response.statusCode(), is(200));

                    response.bodyHandler(buffer-> {
                        assertThat(testContext, new String(buffer.getBytes(), StandardCharsets.UTF_8), is("response"));
                        async.complete();

                    });
                },
                testContext::fail);
    }

    @Test
    public void testPut(TestContext testContext) {
        getMockServerClient().when(
                request()
                        .withMethod("PUT")
                        .withPath("/bucket/key")
                        .withHeader("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
                        .withHeader("X-Amz-Date", "20161110T130214Z")
                        .withHeader("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=3a058beb9174cffbb616894f55a8c5999c376a2ba0851513a635de3952385013")
                        .withBody("test")

        ).respond(
                response()
                        .withStatusCode(200)
                        .withHeader(Header.header("Content-Type", "application/json;charset=UTF-8"))
                        .withBody("response")
        );


        final Async async = testContext.async();
        s3Client.put("bucket", "key",
                Buffer.buffer("test"),
                (response) -> {
                    assertThat(testContext, response.statusCode(), is(200));

                    response.bodyHandler(buffer-> {
                        assertThat(testContext, new String(buffer.getBytes(), StandardCharsets.UTF_8), is("response"));
                        async.complete();

                    });
                },
                testContext::fail);
    }

    @Test
    public void testDelete(TestContext testContext) {
        getMockServerClient().when(
                request()
                        .withMethod("DELETE")
                        .withPath("/bucket/key")
                        .withHeader("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
                        .withHeader("X-Amz-Date", "20161110T130214Z")
                        .withHeader("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=7a119f6c4ce49d2a0eaffd7511d203523b1e2435837f5916d0bd8c08123d4eaa")

        ).respond(
                response()
                        .withStatusCode(200)
                        .withHeader(Header.header("Content-Type", "application/json;charset=UTF-8"))
                        .withBody("response")
        );


        final Async async = testContext.async();
        s3Client.delete("bucket", "key",
                (response) -> {
                    assertThat(testContext, response.statusCode(), is(200));

                    response.bodyHandler(buffer-> {
                        assertThat(testContext, new String(buffer.getBytes(), StandardCharsets.UTF_8), is("response"));
                        async.complete();

                    });
                },
                testContext::fail);
    }

    @Test
    public void testCopy(TestContext testContext) {
        getMockServerClient().when(
                request()
                        .withMethod("PUT")
                        .withPath("/destinationBucket/destinationKey")
                        .withHeader("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
                        .withHeader("X-Amz-Copy-Source", "/sourceBucket/sourceKey")
                        .withHeader("X-Amz-Date", "20161110T130214Z")
                        .withHeader("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-copy-source;x-amz-date, Signature=f4fb57b90ec37c3bb9b437cff5f3fd164d81cbe2ff13f9223b20d3c899f50036")

        ).respond(
                response()
                        .withStatusCode(200)
                        .withHeader(Header.header("Content-Type", "application/json;charset=UTF-8"))
                        .withBody("response")
        );


        final Async async = testContext.async();
        s3Client.copy(
                "sourceBucket", "sourceKey",
                "destinationBucket", "destinationKey",
                (response) -> {
                    assertThat(testContext, response.statusCode(), is(200));

                    response.bodyHandler(buffer-> {
                        assertThat(testContext, new String(buffer.getBytes(), StandardCharsets.UTF_8), is("response"));
                        async.complete();

                    });
                },
                testContext::fail);
    }
}
