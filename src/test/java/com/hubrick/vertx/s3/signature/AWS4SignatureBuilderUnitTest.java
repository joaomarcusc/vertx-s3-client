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
