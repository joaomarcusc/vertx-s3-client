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
package com.hubrick.vertx.s3.model.header;

/**
 * @author Emir Dizdarevic
 * @since 3.0.0
 */
public class ServerSideEncryptionResponseHeaders extends CommonResponseHeaders {

    private String amzServerSideEncription;
    private String amzServerSideEncriptionAwsKmsKeyId;
    private String amzServerSideEncriptionCustomerAlgorithm;
    private String amzServerSideEncriptionCustomerKeyMD5;

    public String getAmzServerSideEncription() {
        return amzServerSideEncription;
    }

    public void setAmzServerSideEncription(String amzServerSideEncription) {
        this.amzServerSideEncription = amzServerSideEncription;
    }

    public String getAmzServerSideEncriptionAwsKmsKeyId() {
        return amzServerSideEncriptionAwsKmsKeyId;
    }

    public void setAmzServerSideEncriptionAwsKmsKeyId(String amzServerSideEncriptionAwsKmsKeyId) {
        this.amzServerSideEncriptionAwsKmsKeyId = amzServerSideEncriptionAwsKmsKeyId;
    }

    public String getAmzServerSideEncriptionCustomerAlgorithm() {
        return amzServerSideEncriptionCustomerAlgorithm;
    }

    public void setAmzServerSideEncriptionCustomerAlgorithm(String amzServerSideEncriptionCustomerAlgorithm) {
        this.amzServerSideEncriptionCustomerAlgorithm = amzServerSideEncriptionCustomerAlgorithm;
    }

    public String getAmzServerSideEncriptionCustomerKeyMD5() {
        return amzServerSideEncriptionCustomerKeyMD5;
    }

    public void setAmzServerSideEncriptionCustomerKeyMD5(String amzServerSideEncriptionCustomerKeyMD5) {
        this.amzServerSideEncriptionCustomerKeyMD5 = amzServerSideEncriptionCustomerKeyMD5;
    }
}
