package com.hubrick.vertx.s3.client;

import com.hubrick.vertx.s3.AbstractFunctionalTest;
import com.hubrick.vertx.s3.S3TestCredentials;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.model.Header;

import java.nio.charset.StandardCharsets;

import static com.hubrick.vertx.s3.VertxMatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * @author marcus
 * @since 1.0.0
 */
public class S3ClientTest extends AbstractFunctionalTest {

    public static final String HOSTNAME = "localhost";
    private S3Client s3Client;

    @Before
    public void setUp() throws Exception {
        final HttpClientOptions clientOptions = new HttpClientOptions();
        clientOptions.setDefaultHost(HOSTNAME);
        clientOptions.setDefaultPort(MOCKSERVER_PORT);
        clientOptions.setMaxPoolSize(10);

        s3Client = new S3Client(
                vertx,
                clientOptions,
                S3TestCredentials.REGION,
                MOCKSERVER_PORT,
                S3TestCredentials.SERVICE_NAME,
                S3TestCredentials.AWS_ACCESS_KEY,
                S3TestCredentials.AWS_SECRET_KEY,
                HOSTNAME);

    }

    @Test
    public void testGet(TestContext testContext) {
        getMockServerClient().when(
                request()
                        .withMethod("GET")
                        .withPath("/bucket/key")
                        .withHeader("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

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
