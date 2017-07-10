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

import com.hubrick.vertx.s3.model.Connection;

/**
 * @author Emir Dizdarevic
 * @since 3.0.0
 */
public class CommonResponseHeaders {

    private Long contentLength;
    private String contentType;
    private Connection connection;
    private String date;
    private String eTag;
    private String server;
    private Boolean amzDeleteMarker;
    private String amzId2;
    private String amzRequestId;
    private String amzVersionId;

    public Long getContentLength() {
        return contentLength;
    }

    public void setContentLength(Long contentLength) {
        this.contentLength = contentLength;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getETag() {
        return eTag;
    }

    public void setETag(String eTag) {
        this.eTag = eTag;
    }

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public Boolean getAmzDeleteMarker() {
        return amzDeleteMarker;
    }

    public void setAmzDeleteMarker(Boolean amzDeleteMarker) {
        this.amzDeleteMarker = amzDeleteMarker;
    }

    public String getAmzId2() {
        return amzId2;
    }

    public void setAmzId2(String amzId2) {
        this.amzId2 = amzId2;
    }

    public String getAmzRequestId() {
        return amzRequestId;
    }

    public void setAmzRequestId(String amzRequestId) {
        this.amzRequestId = amzRequestId;
    }

    public String getAmzVersionId() {
        return amzVersionId;
    }

    public void setAmzVersionId(String amzVersionId) {
        this.amzVersionId = amzVersionId;
    }
}
