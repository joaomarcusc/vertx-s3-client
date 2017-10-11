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

import com.hubrick.vertx.s3.model.CannedAcl;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * @author Emir Dizdarevic
 * @since 2.2.0
 */
public class AclHeadersRequest<T extends AclHeadersRequest> {

    private CannedAcl amzAcl;
    private String amzGrantRead;
    private String amzGrantWrite;
    private String amzGrantReadAcp;
    private String amzGrantWriteAcp;
    private String amzGrantFullControl;

    public T withAmzAcl(CannedAcl amzAcl) {
        this.amzAcl = amzAcl;
        return (T) this;
    }

    public T withAmzGrantRead(String amzGrantRead) {
        this.amzGrantRead = amzGrantRead;
        return (T) this;
    }

    public T withAmzGrantWrite(String amzGrantWrite) {
        this.amzGrantWrite = amzGrantWrite;
        return (T) this;
    }

    public T withAmzGrantReadAcp(String amzGrantReadAcp) {
        this.amzGrantReadAcp = amzGrantReadAcp;
        return (T) this;
    }

    public T withAmzGrantWriteAcp(String amzGrantWriteAcp) {
        this.amzGrantWriteAcp = amzGrantWriteAcp;
        return (T) this;
    }

    public T withAmzGrantFullControl(String amzGrantFullControl) {
        this.amzGrantFullControl = amzGrantFullControl;
        return (T) this;
    }

    public CannedAcl getAmzAcl() {
        return amzAcl;
    }

    public String getAmzGrantRead() {
        return amzGrantRead;
    }

    public String getAmzGrantWrite() {
        return amzGrantWrite;
    }

    public String getAmzGrantReadAcp() {
        return amzGrantReadAcp;
    }

    public String getAmzGrantWriteAcp() {
        return amzGrantWriteAcp;
    }

    public String getAmzGrantFullControl() {
        return amzGrantFullControl;
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
