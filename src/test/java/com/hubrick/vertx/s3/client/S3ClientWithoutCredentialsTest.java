package com.hubrick.vertx.s3.client;

import io.vertx.ext.unit.TestContext;
import org.junit.Test;

/**
 * @author marcus
 * @since 1.0.0
 */
public class S3ClientWithoutCredentialsTest extends AbstractS3ClientTest {

    @Override
    protected void augmentClientOptions(final S3ClientOptions clientOptions) {
    }

    @Test
    public void testGet(TestContext testContext) {
        mockGet();

        verifyGet(testContext);
    }

    @Test
    public void testPut(TestContext testContext) {
        mockPut();

        verifyPut(testContext);
    }

    @Test
    public void testDelete(TestContext testContext) {
        mockDelete();

        verifyDelete(testContext);
    }

    @Test
    public void testCopy(TestContext testContext) {
        mockCopy();

        verifyCopy(testContext);

    }
}
