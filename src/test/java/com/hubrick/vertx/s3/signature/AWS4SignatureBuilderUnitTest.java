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
package com.hubrick.vertx.s3.signature;

import com.hubrick.vertx.s3.S3TestCredentials;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author marcus
 * @since 1.0.0
 */
public class AWS4SignatureBuilderUnitTest {

    private static final ZonedDateTime TIME = ZonedDateTime.from(DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssX").parse("20150830T123600Z"));

    @Test
    public void testEscaping() {
        final AWS4SignatureBuilder aws4SignatureBuilder = AWS4SignatureBuilder.builder(TIME, S3TestCredentials.REGION, S3TestCredentials.SERVICE_NAME)
                .awsSecretKey(S3TestCredentials.AWS_SECRET_KEY)
                .httpRequestMethod("PUT")
                .canonicalUri("/test:test/");

        assertThat(aws4SignatureBuilder.makeCanonicalRequest(), is("PUT\n/test%3Atest/\n\n\n\nUNSIGNED-PAYLOAD"));
    }

}
