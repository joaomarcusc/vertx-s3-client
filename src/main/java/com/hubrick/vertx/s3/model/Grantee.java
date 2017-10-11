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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.net.URL;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Emir Dizdarevic
 * @since 3.2.0
 */
@XmlRootElement(name = "Grantee")
@XmlAccessorType(XmlAccessType.FIELD)
public class Grantee {

    @XmlElement(name = "ID")
    private String id;

    @XmlElement(name = "DisplayName")
    private String displayName;

    @XmlElement(name = "URI")
    private URL uri;

    @XmlElement(name = "EmailAddress")
    private String emailAddress;

    protected Grantee() {}

    public Grantee(String id, String displayName) {
        checkNotNull(StringUtils.trimToNull(id), "id must not be null");
        checkNotNull(StringUtils.trimToNull(displayName), "displayName must not be null");

        this.id = id;
        this.displayName = displayName;
    }

    public Grantee(URL uri) {
        checkNotNull(uri, "uri must not be null");

        this.uri = uri;
    }

    public Grantee(String emailAddress) {
        checkNotNull(StringUtils.trimToNull(emailAddress), "emailAddress must not be null");

        this.emailAddress = emailAddress;
    }

    public String getId() {
        return id;
    }

    public String getDisplayName() {
        return displayName;
    }

    public URL getUri() {
        return uri;
    }

    public String getEmailAddress() {
        return emailAddress;
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
