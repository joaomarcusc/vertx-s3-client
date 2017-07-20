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
public class CopyObjectResponseHeaders extends ServerSideEncryptionResponseHeaders {

    private String amzExpiration;
    private String amzCopySourceVersionId;

    public String getAmzExpiration() {
        return amzExpiration;
    }

    public void setAmzExpiration(String amzExpiration) {
        this.amzExpiration = amzExpiration;
    }

    public String getAmzCopySourceVersionId() {
        return amzCopySourceVersionId;
    }

    public void setAmzCopySourceVersionId(String amzCopySourceVersionId) {
        this.amzCopySourceVersionId = amzCopySourceVersionId;
    }
}
