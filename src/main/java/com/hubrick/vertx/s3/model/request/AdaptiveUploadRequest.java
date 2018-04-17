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

import com.hubrick.vertx.s3.model.StorageClass;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Emir Dizdarevic
 * @since 3.1.0
 */
public class AdaptiveUploadRequest extends AclHeadersRequest<AdaptiveUploadRequest> {

    private final ReadStream<Buffer> readStream;

    private String cacheControl;
    private String contentDisposition;
    private String contentEncoding;
    private String contentType;
    private String contentMD5;
    private String expires;

    private MultiMap amzMeta = MultiMap.caseInsensitiveMultiMap();
    private StorageClass amzStorageClass;
    private String amzTagging;
    private String amzWebsiteRedirectLocation;

    private Integer writeQueueMaxSize;
    private Integer bufferSize;

    public AdaptiveUploadRequest(ReadStream<Buffer> readStream) {
        checkNotNull(readStream, "readStream must not be null");

        this.readStream = readStream;
    }

    public AdaptiveUploadRequest withCacheControl(String cacheControl) {
        this.cacheControl = cacheControl;
        return this;
    }

    public AdaptiveUploadRequest withContentDisposition(String contentDisposition) {
        this.contentDisposition = contentDisposition;
        return this;
    }

    public AdaptiveUploadRequest withContentEncoding(String contentEncoding) {
        this.contentEncoding = contentEncoding;
        return this;
    }

    public AdaptiveUploadRequest withContentType(String contentType) {
        this.contentType = contentType;
        return this;
    }


    public AdaptiveUploadRequest withContentMD5(String contentMD5) {
        this.contentMD5 = contentMD5;
        return this;
    }

    public AdaptiveUploadRequest withExpires(String expires) {
        this.expires = expires;
        return this;
    }

    public AdaptiveUploadRequest withAmzMeta(String key, String value) {
        this.amzMeta.add(key, value);
        return this;
    }

    public AdaptiveUploadRequest withAmzMeta(MultiMap amzMeta) {
        this.amzMeta.addAll(amzMeta);
        return this;
    }

    public AdaptiveUploadRequest withAmzStorageClass(StorageClass amzStorageClass) {
        this.amzStorageClass = amzStorageClass;
        return this;
    }

    public AdaptiveUploadRequest withAmzTagging(String amzTagging) {
        this.amzTagging = amzTagging;
        return this;
    }

    public AdaptiveUploadRequest withAmzWebsiteRedirectLocation(String amzWebsiteRedirectLocation) {
        this.amzWebsiteRedirectLocation = amzWebsiteRedirectLocation;
        return this;
    }

    public AdaptiveUploadRequest withWriteQueueMaxSize(Integer writeQueueMaxSize) {
        this.writeQueueMaxSize = writeQueueMaxSize;
        return this;
    }

    public AdaptiveUploadRequest withBufferSize(Integer bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    public ReadStream<Buffer> getReadStream() {
        return readStream;
    }

    public String getCacheControl() {
        return cacheControl;
    }

    public String getContentDisposition() {
        return contentDisposition;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }

    public String getContentType() {
        return contentType;
    }

    public String getContentMD5() {
        return contentMD5;
    }

    public String getExpires() {
        return expires;
    }

    public MultiMap getAmzMeta() {
        return amzMeta;
    }

    public StorageClass getAmzStorageClass() {
        return amzStorageClass;
    }

    public String getAmzTagging() {
        return amzTagging;
    }

    public String getAmzWebsiteRedirectLocation() {
        return amzWebsiteRedirectLocation;
    }

    public Integer getWriteQueueMaxSize() {
        return writeQueueMaxSize;
    }

    public Integer getBufferSize() {
        return bufferSize;
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
