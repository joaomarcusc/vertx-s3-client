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

import com.google.common.collect.ImmutableMap;
import com.hubrick.vertx.s3.model.AccessControlPolicy;
import com.hubrick.vertx.s3.model.Grant;
import com.hubrick.vertx.s3.model.Grantee;
import com.hubrick.vertx.s3.model.Owner;
import com.hubrick.vertx.s3.model.Permission;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.junit.Test;
import org.mockserver.model.Header;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import static com.hubrick.vertx.s3.VertxMatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

/**
 * @author marcus
 * @since 1.0.0
 */
public class S3ClientSignedContentTest extends AbstractS3ClientTest {

    @Override
    protected void augmentClientOptions(final S3ClientOptions clientOptions) {
        addCredentials(clientOptions);

        clientOptions.setSignPayload(true);
    }

    @Test
    public void testGetObject(TestContext testContext) throws IOException {
        mockGetObject(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=1312c94db8c2ad1d2d56d4c6bb16cdb7c0485b100b2baf2d8aabbb3fec0075fb")
        );

        verifyGetObject(testContext);
    }

    @Test
    public void testGetObjectError(TestContext testContext) throws IOException {
        mockGetObjectErrorResponse(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=1312c94db8c2ad1d2d56d4c6bb16cdb7c0485b100b2baf2d8aabbb3fec0075fb")
        );

        verifyGetObjectErrorResponse(testContext);
    }

    @Test
    public void testGetObjectAcl(TestContext testContext) throws IOException {
        final AccessControlPolicy accessControlPolicy = new AccessControlPolicy(new Owner("someid", "somedisplayname"), Collections.singletonList(new Grant(new Grantee("id", "displayname"), Permission.FULL_CONTROL)));
        mockGetObjectAcl(
                accessControlPolicy,
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=e934d94af24578657a02c3ef1ee1d8d946c12f6e5ec11f3637fc99e5c118492e")
        );

        verifyGetObjectAcl(testContext, accessControlPolicy);
    }

    @Test
    public void testGetObjectAclError(TestContext testContext) throws IOException {
        mockGetObjectAclErrorResponse(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=e934d94af24578657a02c3ef1ee1d8d946c12f6e5ec11f3637fc99e5c118492e")
        );

        verifyGetObjectAclErrorResponse(testContext);
    }

    @Test
    public void testHeadObject(TestContext testContext) throws IOException {
        mockHeadObject(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5f5f4166a85b6963678cc951d1b851223869717a10d77465c85278f717e3073d")
        );

        verifyHeadObject(testContext);
    }

    @Test
    public void testHeadObjectError(TestContext testContext) throws IOException {
        mockHeadObjectErrorResponse(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5f5f4166a85b6963678cc951d1b851223869717a10d77465c85278f717e3073d")
        );

        verifyHeadObjectErrorResponse(testContext);
    }

    @Test
    public void testPutObject(TestContext testContext) throws IOException {
        mockPutObject(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=ec91365b21b715a7f981a89f61e2ef2c0a73cce5998b4272272f68a82d94e055")
        );

        verifyPutObject(testContext);
    }

    @Test
    public void testPutObjectError(TestContext testContext) throws IOException {
        mockPutObjectErrorResponse(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=ec91365b21b715a7f981a89f61e2ef2c0a73cce5998b4272272f68a82d94e055")
        );

        verifyPutObjectErrorResponse(testContext);
    }

    @Test
    public void testPutObjectAclWithHeaders(TestContext testContext) throws IOException {
        mockPutObjectAclWithHeaders(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-acl;x-amz-date, Signature=7365ade7156bdd7e804f064495b189e5a305508a12538c244324f8fa4704624c"),
                Header.header("X-Amz-Acl", "private")
        );

        verifyPutObjectAclWithHeaders(testContext);
    }

    @Test
    public void testPutObjectAclWithHeadersError(TestContext testContext) throws IOException {
        mockPutObjectAclWithHeadersErrorResponse(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-acl;x-amz-date, Signature=7365ade7156bdd7e804f064495b189e5a305508a12538c244324f8fa4704624c"),
                Header.header("X-Amz-Acl", "private")
        );

        verifyPutObjectAclWithHeadersErrorResponse(testContext);
    }

    @Test
    public void testPutObjectAclWithBody(TestContext testContext) throws IOException {
        final AccessControlPolicy accessControlPolicy = new AccessControlPolicy(new Owner("someid", "somedisplayname"), Collections.singletonList(new Grant(new Grantee("id", "displayname"), Permission.FULL_CONTROL)));
        mockPutObjectAclWithBody(
                accessControlPolicy,
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "0f34495cc4a765bd15a14ef867ac12f7c842a27d10aecc92f3cb442377a63aed"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=d81ccb9bc9f7bfaf3fb5780100cc820a32494fb054d8b82549066b0484c7f4b7")
        );

        verifyPutObjectAclWithBody(accessControlPolicy, testContext);
    }

    @Test
    public void testPutObjectAclWithBodyError(TestContext testContext) throws IOException {
        final AccessControlPolicy accessControlPolicy = new AccessControlPolicy(new Owner("someid", "somedisplayname"), Collections.singletonList(new Grant(new Grantee("id", "displayname"), Permission.FULL_CONTROL)));
        mockPutObjectAclWithBodyErrorResponse(
                accessControlPolicy,
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "0f34495cc4a765bd15a14ef867ac12f7c842a27d10aecc92f3cb442377a63aed"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=d81ccb9bc9f7bfaf3fb5780100cc820a32494fb054d8b82549066b0484c7f4b7")
        );

        verifyPutObjectAclWithBodyErrorResponse(accessControlPolicy, testContext);
    }

    @Test
    public void testInitMultipartUpload(TestContext testContext) throws IOException {
        mockInitMultipartUpload(
                UUID.randomUUID().toString(),
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=592f1e6154deaa7d9b4e3dce4b4b4df68ebc3f92aa66d6eec83ee65150c31012")
        );

        verifyInitMultipartUpload(testContext);
    }

    @Test
    public void testInitMultipartUploadError(TestContext testContext) throws IOException {
        mockInitMultipartUploadErrorResponse(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=592f1e6154deaa7d9b4e3dce4b4b4df68ebc3f92aa66d6eec83ee65150c31012")
        );

        verifyInitMultipartUploadErrorResponse(testContext);
    }


    @Test
    public void testContinueMultipartUpload(TestContext testContext) throws IOException {
        mockContinueMultipartUpload(
                1,
                "someid",
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "1307990e6ba5ca145eb35e99182a9bec46531bc54ddf656a602c780fa0240dee"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=457b75b3b0ef6b6c084a60ff9dbbe236423dfecb8a8558d5529f6a8ffbd7b07a")
        );

        verifyContinueMultipartUpload(testContext, 1, "someid");
    }

    @Test
    public void testContinueMultipartUploadError(TestContext testContext) throws IOException {
        mockContinueMultipartUploadErrorResponse(
                1,
                "someid",
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "85738f8f9a7f1b04b5329c590ebcb9e425925c6d0984089c43a022de4f19c281"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=34686ec1d1a077b9dd7e8324502ef895b64c6e10eadf47c86b6359636fdb7802")
        );

        verifyContinueMultipartUploadErrorResponse(testContext, 1, "someid");
    }

    @Test
    public void testCompleteMultipartUpload(TestContext testContext) throws IOException {
        mockCompleteMultipartUpload(
                "someid",
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e1a73c03534c461040120d668247de00719d1563488097307aa3926a371eb4b2"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=aa83043e3d30e17f1a889ed3e75155117a11ef19dd8607e6565254363e61631a")
        );

        verifyCompleteMultipartUpload(testContext, "someid");
    }

    @Test
    public void testCompleteMultipartUploadError(TestContext testContext) throws IOException {
        mockCompleteMultipartUploadErrorResponse(
                "someid",
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e1a73c03534c461040120d668247de00719d1563488097307aa3926a371eb4b2"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=aa83043e3d30e17f1a889ed3e75155117a11ef19dd8607e6565254363e61631a")
        );

        verifyCompleteMultipartUploadErrorResponse(testContext, "someid");
    }

    @Test
    public void testAbortMultipartUpload(TestContext testContext) throws IOException {
        mockAbortMultipartUpload(
                "someid",
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=dcff03d4bd9b73856f1d91406a366e751e996d5239764e5ca3d64ff9b1aae18d")
        );

        verifyAbortMultipartUpload(testContext, "someid");
    }

    @Test
    public void testAbortMultipartUploadError(TestContext testContext) throws IOException {
        mockAbortMultipartUploadErrorResponse(
                "someid",
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=dcff03d4bd9b73856f1d91406a366e751e996d5239764e5ca3d64ff9b1aae18d")
        );

        verifyAbortMultipartUploadErrorResponse(testContext, "someid");
    }

    @Test
    public void testMultipartUpload(TestContext testContext) throws IOException {
        mockInitMultipartUpload(
                "someId",
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=592f1e6154deaa7d9b4e3dce4b4b4df68ebc3f92aa66d6eec83ee65150c31012")
        );

        mockContinueMultipartUpload(
                1,
                "someid",
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "290f493c44f5d63d06b374d0a5abd292fae38b92cab2fae5efefe1b0e9347f56"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=339943aca690a673c5e0c289f6d104fdabc55ee8e65b0483d6ea0cfde41e0d24")
        );

        mockCompleteMultipartUpload(
                "someid",
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "335f4b7fbeff95be2489677a4d3808af467e671d7f912acbe7e430791bb816a5"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=ed509a886966b1d7d9aa1565c85e8d98b75a84f7e87d9b750dfaed0cb1d69f6f")
        );

        final Async async = testContext.async();
        callInitMultipartUpload(testContext, response -> {
            assertThat(testContext, response.getData(), notNullValue());
            assertThat(testContext, response.getHeader(), notNullValue());

            response.getData().endHandler(success -> async.complete());
            response.getData().exceptionHandler(event -> testContext.fail(event));
            response.getData().write(Buffer.buffer("some content".getBytes()));
            response.getData().end();
        });
    }

    @Test
    public void testDeleteObject(TestContext testContext) throws IOException {
        mockDeleteObject(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=bc61c03c7fbd958aa69920984856ca9ec5454c6714524b39fa637e1837e96355")
        );

        verifyDeleteObject(testContext);
    }

    @Test
    public void testDeleteObjectError(TestContext testContext) throws IOException {
        mockDeleteObjectErrorResponse(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=bc61c03c7fbd958aa69920984856ca9ec5454c6714524b39fa637e1837e96355")
        );

        verifyDeleteObjectErrorResponse(testContext);
    }


    @Test
    public void testCopyObject(TestContext testContext) throws IOException {
        mockCopyObject(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-copy-source;x-amz-date, Signature=f852af4f877c4f1522af892d11a4bd9a6a42327a67877916a8d57c3339f46d0b")
        );

        verifyCopyObject(testContext);
    }

    @Test
    public void testCopyObjectError(TestContext testContext) throws IOException {
        mockCopyObjectErrorResponse(
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-copy-source;x-amz-date, Signature=f852af4f877c4f1522af892d11a4bd9a6a42327a67877916a8d57c3339f46d0b")
        );

        verifyCopyObjectErrorResponse(testContext);
    }

    @Test
    public void testGetBucket(TestContext testContext) throws IOException {
        mockGetBucket(
                ImmutableMap.of(
                        "list-type", Collections.singletonList("2"),
                        "continuation-token", Collections.singletonList("14HF6Dfbr92F1EYlZIrMwxPYKQl5lD/mbwiw5+Nlrn1lYIZX3YGzo16Dgz+dxbxFeNGmLsnzwnbbuQM0CMl0krVwh8TBj8nCmNtq/iQCK6gzln8z3U4C71Mh2HyEMHcMgrZGR/akosVql7/AIctj6rA==")
                ),
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=62c51118953e03169f4d723c3ffc4fdcc3a30831b898f962dffd423264dae75b")
        );

        verifyGetBucket(testContext);
    }

    @Test
    public void testGetBucketError(TestContext testContext) throws IOException {
        mockGetBucketErrorResponse(
                ImmutableMap.of(
                        "list-type", Collections.singletonList("2"),
                        "continuation-token", Collections.singletonList("14HF6Dfbr92F1EYlZIrMwxPYKQl5lD/mbwiw5+Nlrn1lYIZX3YGzo16Dgz+dxbxFeNGmLsnzwnbbuQM0CMl0krVwh8TBj8nCmNtq/iQCK6gzln8z3U4C71Mh2HyEMHcMgrZGR/akosVql7/AIctj6rA==")
                ),
                Header.header("X-Amz-Date", "20161110T130214Z"),
                Header.header("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Header.header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20161110/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=62c51118953e03169f4d723c3ffc4fdcc3a30831b898f962dffd423264dae75b")
        );

        verifyGetBucketErrorResponse(testContext);
    }
}
