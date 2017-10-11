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
package com.hubrick.vertx.s3.model;

/**
 * @author Emir Dizdarevic
 * @since 3.2.0
 */
public enum CannedAcl {
    PRIVATE("private"),
    PUBLIC_READ("public-read"),
    PUBLIC_READ_WRITE("public-read-write"),
    AWS_EXEC_READ("aws-exec-read"),
    AUTHENTCATED_READ("authenticated-read"),
    BUCKET_OWNER_READ("bucket-owner-read"),
    BUCKET_OWNER_FULL_CONTROL("bucket-owner-full-control"),
    LOG_DELIVERY_WRITE("log-delivery-write");

    private final String value;

    CannedAcl(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
