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

import com.hubrick.vertx.s3.model.header.CommonResponseHeaders;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Emir Dizdarevic
 * @since 3.0.0
 */
public class ResponseWithBody<H extends CommonResponseHeaders, B> implements Response<H, B> {

    private final H header;
    private final B data;

    public ResponseWithBody(H header, B data) {
        checkNotNull(header, "header must not be null");
        checkNotNull(data, "data must not be null");

        this.header = header;
        this.data = data;
    }

    public H getHeader() {
        return header;
    }

    public B getData() {
        return data;
    }
}
