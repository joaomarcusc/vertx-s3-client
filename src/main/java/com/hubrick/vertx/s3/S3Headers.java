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
package com.hubrick.vertx.s3;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public enum S3Headers {
    ACL_HEADER("X-Amz-Acl"),
    COPY_SOURCE_HEADER("X-Amz-Copy-Source"),
    DATE("X-Amz-Date"),
    CONTENT_SHA("X-Amz-Content-Sha256");

    private static final Map<String, S3Headers> REVERSE_LOOKUP = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        for(S3Headers s3Headers : values()) {
            REVERSE_LOOKUP.put(s3Headers.getValue(), s3Headers);
        }
    }

    private final String value;

    S3Headers(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public S3Headers fromString(String value) {
        return REVERSE_LOOKUP.get(value);
    }
}
