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

import com.hubrick.vertx.s3.model.Part;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Emir Dizdarevic
 * @since 3.0.0
 */
@XmlRootElement(name = "CompleteMultipartUpload")
@XmlAccessorType(XmlAccessType.FIELD)
public class CompleteMultipartUploadRequest {

    @XmlTransient
    private String uploadId;

    @XmlElement(name = "Part", type = Part.class)
    private List<Part> parts;

    protected CompleteMultipartUploadRequest() {}

    public CompleteMultipartUploadRequest(String uploadId, List<Part> parts) {
        checkNotNull(StringUtils.trimToNull(uploadId), "uploadId must not be null");
        checkNotNull(parts, "parts must not be null");

        this.uploadId = uploadId;
        this.parts = parts;
    }

    public String getUploadId() {
        return uploadId;
    }

    public List<Part> getParts() {
        return parts;
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
