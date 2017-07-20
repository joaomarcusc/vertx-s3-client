/**
 * Copyright (C) 2016 Etaia AS (oss@hubrick.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hubrick.vertx.s3.client;

import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.mockserver.model.Header;

import java.io.IOException;
import java.util.UUID;

/**
 * @author marcus
 * @since 1.0.0
 */
public class S3ClientWithCredentialsTest extends AbstractS3ClientTest {

    @Override
    protected void augmentClientOptions(final S3ClientOptions clientOptions) {
        addCredentials(clientOptions);
    }

    @Test
    public void testGetObject(TestContext testContext) throws IOException {
        mockGetObject(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=77f53b6d1eb26273da59491e98ea10989a006a57620d7351be44b596a348d699")
        );

        verifyGetObject(testContext);
    }

    @Test
    public void testHeadObject(TestContext testContext) throws IOException {
        mockHeadObject(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=b77c3b79a471ca7a096af18fa56affe4a45589a6d732770b3a30f54aa6e1399d")
        );

        verifyHeadObject(testContext);
    }

    @Test
    public void testPutObject(TestContext testContext) throws IOException {
        mockPutObject(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=3a058beb9174cffbb616894f55a8c5999c376a2ba0851513a635de3952385013")
        );

        verifyPutObject(testContext);
    }

    @Test
    public void testInitMultipartUpload(TestContext testContext) throws IOException {
        mockInitMultipartUpload(
                UUID.randomUUID().toString(),
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=18d1d5fe4511763bc95c86e532e492467848c80c1b8f0cab745cf2ac2a7dcc5f")
        );

        verifyInitMultipartUpload(testContext);
    }

    @Test
    public void testContinueMultipartUpload(TestContext testContext) throws IOException {
        mockContinueMultipartUpload(
                1,
                "someid",
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=2e50b12ee253fa9e8be77d64d3a06adbf066fe9eef3d97332676bdacf5860e2a")
        );

        verifyContinueMultipartUpload(testContext, 1, "someid");
    }

    @Test
    public void testCompleteMultipartUpload(TestContext testContext) throws IOException {
        mockCompleteMultipartUpload(
                "someid",
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=bd1a4688a9f54536517dbb4a2a3ddfcd762f955950170a1dad75085a990586e4")
        );

        verifyCompleteMultipartUpload(testContext, "someid");
    }

    @Test
    public void testAbortMultipartUpload(TestContext testContext) throws IOException {
        mockAbortMultipartUpload(
                "someid",
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=f5b73d8b2a130264055219a27f1e9dece8f955431b282a5f8c930846aeabd59a")
        );

        verifyAbortMultipartUpload(testContext, "someid");
    }

    @Test
    public void testDeleteObject(TestContext testContext) throws IOException {
        mockDeleteObject(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=7a119f6c4ce49d2a0eaffd7511d203523b1e2435837f5916d0bd8c08123d4eaa")
        );

        verifyDeleteObject(testContext);
    }

    @Test
    public void testCopyObject(TestContext testContext) throws IOException {
        mockCopyObject(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-copy-source;x-amz-date, Signature=f4fb57b90ec37c3bb9b437cff5f3fd164d81cbe2ff13f9223b20d3c899f50036")
        );

        verifyCopyObject(testContext);
    }
}
