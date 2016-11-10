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
public class S3ClientSignedContentTest extends AbstractFunctionalTest {

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
        clientOptions.setSignPayload(true);

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
                        .withHeader("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
                        .withHeader("X-Amz-Date", "20161110T130214Z")
                        .withHeader("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=1312c94db8c2ad1d2d56d4c6bb16cdb7c0485b100b2baf2d8aabbb3fec0075fb")

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
                        .withHeader("X-Amz-Content-Sha256", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08")
                        .withHeader("X-Amz-Date", "20161110T130214Z")
                        .withHeader("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=ec91365b21b715a7f981a89f61e2ef2c0a73cce5998b4272272f68a82d94e055")
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
                        .withHeader("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
                        .withHeader("X-Amz-Date", "20161110T130214Z")
                        .withHeader("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=bc61c03c7fbd958aa69920984856ca9ec5454c6714524b39fa637e1837e96355")

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
                        .withHeader("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
                        .withHeader("X-Amz-Copy-Source", "/sourceBucket/sourceKey")
                        .withHeader("X-Amz-Date", "20161110T130214Z")
                        .withHeader("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-copy-source;x-amz-date, Signature=f852af4f877c4f1522af892d11a4bd9a6a42327a67877916a8d57c3339f46d0b")

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
