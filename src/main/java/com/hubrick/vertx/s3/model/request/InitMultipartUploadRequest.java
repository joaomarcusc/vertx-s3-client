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
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author Emir Dizdarevic
 * @since 3.0.0
 */
public class InitMultipartUploadRequest extends AclHeadersRequest<InitMultipartUploadRequest> {

    private String cacheControl;
    private String contentDisposition;
    private String contentEncoding;
    private String contentType;
    private String expires;

    private MultiMap amzMeta = MultiMap.caseInsensitiveMultiMap();
    private StorageClass amzStorageClass;
    private String amzWebsiteRedirectLocation;

    public InitMultipartUploadRequest withCacheControl(String cacheControl) {
        this.cacheControl = cacheControl;
        return this;
    }

    public InitMultipartUploadRequest withContentDisposition(String contentDisposition) {
        this.contentDisposition = contentDisposition;
        return this;
    }

    public InitMultipartUploadRequest withContentEncoding(String contentEncoding) {
        this.contentEncoding = contentEncoding;
        return this;
    }

    public InitMultipartUploadRequest withContentType(String contentType) {
        this.contentType = contentType;
        return this;
    }

    public InitMultipartUploadRequest withExpires(String expires) {
        this.expires = expires;
        return this;
    }

    public InitMultipartUploadRequest withAmzMeta(String key, String value) {
        this.amzMeta.add(key, value);
        return this;
    }

    public InitMultipartUploadRequest withAmzMeta(MultiMap amzMeta) {
        this.amzMeta.addAll(amzMeta);
        return this;
    }

    public InitMultipartUploadRequest withAmzStorageClass(StorageClass amzStorageClass) {
        this.amzStorageClass = amzStorageClass;
        return this;
    }

    public InitMultipartUploadRequest withAmzWebsiteRedirectLocation(String amzWebsiteRedirectLocation) {
        this.amzWebsiteRedirectLocation = amzWebsiteRedirectLocation;
        return this;
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

    public String getExpires() {
        return expires;
    }

    public MultiMap getAmzMeta() {
        return amzMeta;
    }

    public StorageClass getAmzStorageClass() {
        return amzStorageClass;
    }

    public String getAmzWebsiteRedirectLocation() {
        return amzWebsiteRedirectLocation;
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
