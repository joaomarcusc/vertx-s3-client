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

import com.hubrick.vertx.s3.model.ReplicationStatus;
import com.hubrick.vertx.s3.model.StorageClass;
import io.vertx.core.MultiMap;

/**
 * @author Emir Dizdarevic
 * @since 3.0.0
 */
public class GetObjectResponseHeaders extends ServerSideEncryptionResponseHeaders {

    private String amzExpiration;
    private MultiMap amzMeta = MultiMap.caseInsensitiveMultiMap();
    private ReplicationStatus amzReplicationStatus;
    private String amzRestore;
    private StorageClass amzStorageClass;
    private Integer amzTaggingCount;
    private String amzWebsiteRedirectLocation;

    public String getAmzExpiration() {
        return amzExpiration;
    }

    public void setAmzExpiration(String amzExpiration) {
        this.amzExpiration = amzExpiration;
    }

    public MultiMap getAmzMeta() {
        return amzMeta;
    }

    public void setAmzMeta(MultiMap amzMeta) {
        this.amzMeta = amzMeta;
    }

    public ReplicationStatus getAmzReplicationStatus() {
        return amzReplicationStatus;
    }

    public void setAmzReplicationStatus(ReplicationStatus amzReplicationStatus) {
        this.amzReplicationStatus = amzReplicationStatus;
    }

    public String getAmzRestore() {
        return amzRestore;
    }

    public void setAmzRestore(String amzRestore) {
        this.amzRestore = amzRestore;
    }

    public StorageClass getAmzStorageClass() {
        return amzStorageClass;
    }

    public void setAmzStorageClass(StorageClass amzStorageClass) {
        this.amzStorageClass = amzStorageClass;
    }

    public Integer getAmzTaggingCount() {
        return amzTaggingCount;
    }

    public void setAmzTaggingCount(Integer amzTaggingCount) {
        this.amzTaggingCount = amzTaggingCount;
    }

    public String getAmzWebsiteRedirectLocation() {
        return amzWebsiteRedirectLocation;
    }

    public void setAmzWebsiteRedirectLocation(String amzWebsiteRedirectLocation) {
        this.amzWebsiteRedirectLocation = amzWebsiteRedirectLocation;
    }
}
