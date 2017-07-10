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
package com.hubrick.vertx.s3.model.request;

import io.vertx.core.buffer.Buffer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Emir Dizdarevic
 * @since 3.0.0
 */
public class ContinueMultipartUploadRequest {

    private final Buffer data;
    private final Integer partNumber;
    private final String uploadId;

    private String contentMD5;

    public ContinueMultipartUploadRequest(Buffer data, Integer partNumber, String uploadId) {
        checkNotNull(data, "data must not be null");
        checkNotNull(partNumber, "partNumber must not be null");
        checkNotNull(StringUtils.trimToNull(uploadId), "uploadId must not be null");

        this.data = data;
        this.partNumber = partNumber;
        this.uploadId = uploadId;
    }

    public ContinueMultipartUploadRequest withContentMD5(String contentMD5) {
        this.contentMD5 = contentMD5;
        return this;
    }

    public Buffer getData() {
        return data;
    }

    public Integer getPartNumber() {
        return partNumber;
    }

    public String getUploadId() {
        return uploadId;
    }

    public String getContentMD5() {
        return contentMD5;
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
