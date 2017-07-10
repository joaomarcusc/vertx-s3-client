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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author Emir Dizdarevic
 * @since 2.1.0
 */
public class HeadObjectRequest {

    private String range;
    private String ifModifiedSince;
    private String ifUnmodifiedSince;
    private String ifMatch;
    private String ifNoneMatch;

    public HeadObjectRequest withRange(String range) {
        this.range = range;
        return this;
    }

    public HeadObjectRequest withIfModifiedSince(String ifModifiedSince) {
        this.ifModifiedSince = ifModifiedSince;
        return this;
    }

    public HeadObjectRequest withIfUnmodifiedSince(String ifUnmodifiedSince) {
        this.ifUnmodifiedSince = ifUnmodifiedSince;
        return this;
    }

    public HeadObjectRequest withIfMatch(String ifMatch) {
        this.ifMatch = ifMatch;
        return this;
    }

    public HeadObjectRequest withIfNoneMatch(String ifNoneMatch) {
        this.ifNoneMatch = ifNoneMatch;
        return this;
    }

    public String getRange() {
        return range;
    }

    public String getIfModifiedSince() {
        return ifModifiedSince;
    }

    public String getIfUnmodifiedSince() {
        return ifUnmodifiedSince;
    }

    public String getIfMatch() {
        return ifMatch;
    }

    public String getIfNoneMatch() {
        return ifNoneMatch;
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
