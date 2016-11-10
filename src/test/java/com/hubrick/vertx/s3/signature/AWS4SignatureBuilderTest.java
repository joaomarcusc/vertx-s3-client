package com.hubrick.vertx.s3.signature;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import com.hubrick.vertx.s3.S3TestCredentials;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author marcus
 * @since 1.0.0
 */
@RunWith(Parameterized.class)
public class AWS4SignatureBuilderTest {

    private static final Set<String> TESTCASES = ImmutableSet.of(
            "get-header-key-duplicate",
            "get-header-value-order",
            "get-header-value-trim",
            "get-unreserved",
            "get-utf8",
            "get-vanilla",
            "get-vanilla-empty-query-key",
            "get-vanilla-query",
            "get-vanilla-query-order-key-case",
            "get-vanilla-query-unreserved",
            "get-vanilla-utf8-query",
            "post-header-key-case",
            "post-header-key-sort",
            "post-header-value-case",
            "post-vanilla",
            "post-vanilla-empty-query-value",
            "post-vanilla-query",
            "post-vanilla-query-nonunreserved",
            "post-vanilla-query-space",
            "post-x-www-form-urlencoded",
            "post-x-www-form-urlencoded-parameters"
    );

    private static final ZonedDateTime TIME = ZonedDateTime.from(DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssX").parse("20150830T123600Z"));
    private final String basename;

    public AWS4SignatureBuilderTest(final String basename) {
        this.basename = basename;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> getBasenames() {
        return TESTCASES.stream().map(testcase -> new Object[]{testcase}).collect(Collectors.toList());
    }

    private AWS4SignatureBuilder initBuilder() {
        return AWS4SignatureBuilder.builder(TIME, S3TestCredentials.REGION, S3TestCredentials.SERVICE_NAME)
                .awsSecretKey(S3TestCredentials.AWS_SECRET_KEY);
    }

    private void initWithRequest(final AWS4SignatureBuilder signatureBuilder) throws Exception {
        final String request = loadFile("req");

        final Iterator<String> lines = Splitter.on('\n').split(request).iterator();

        final String httpLine = lines.next();

        final Iterator<String> httpParts = Splitter.on(' ').split(httpLine).iterator();
        final String method = httpParts.next();
        signatureBuilder.httpRequestMethod(method);

        final String requestUri = httpParts.next();
        if (requestUri.contains("?")) {
            final String canonicalUri = requestUri.substring(0,requestUri.indexOf('?'));
            signatureBuilder.canonicalUri(canonicalUri);
            signatureBuilder.canonicalQueryString(requestUri.substring(canonicalUri.length()+1));
        } else {
            signatureBuilder.canonicalUri(requestUri);
        }

        String line;
        do {
            line = lines.next();

            if (StringUtils.isBlank(line)) {
                break;
            }

            final Iterator<String> headerSplit = Splitter.on(':').split(line).iterator();
            signatureBuilder.header(headerSplit.next(), headerSplit.next());

        } while (lines.hasNext());

        final StringBuilder content = new StringBuilder();
        while (lines.hasNext()) {
            content.append(lines.next());

            if (lines.hasNext()) {
                content.append('\n');
            }
        }

        signatureBuilder.payload(content.toString());
    }

    private String loadFile(final String suffix) throws IOException {
        final String inputFile = "/aws4_testsuite/" + basename + "/" + basename + "." + suffix;
        final InputStream resourceAsStream = this.getClass().getResourceAsStream(inputFile);

        Preconditions.checkState(resourceAsStream != null, "Could not load " + inputFile);
        return CharStreams.toString(new InputStreamReader(resourceAsStream, Charsets.UTF_8));
    }

    @Test
    public void testCanonical() throws Exception {
        final AWS4SignatureBuilder signatureBuilder = makeInitializedBuilder();

        final String createdCanonicalRequest = signatureBuilder.makeCanonicalRequest();
        final String expected = loadFile("creq");

        assertThat(createdCanonicalRequest, is(expected));
    }

    private AWS4SignatureBuilder makeInitializedBuilder() throws Exception {
        final AWS4SignatureBuilder signatureBuilder = initBuilder();
        initWithRequest(signatureBuilder);
        return signatureBuilder;
    }

    @Test
    public void testSignString() throws Exception {
        final AWS4SignatureBuilder signatureBuilder = makeInitializedBuilder();

        final String signString = signatureBuilder.makeSignatureString();
        final String expected = loadFile("sts");

        assertThat(signString, is(expected));
    }

    @Test
    public void testAuthorizationHeader() throws Exception {
        final AWS4SignatureBuilder signatureBuilder = makeInitializedBuilder();

        final String authorizationHeaderValue = signatureBuilder.buildAuthorizationHeaderValue(S3TestCredentials.AWS_ACCESS_KEY);
        final String expected = loadFile("authz");

        assertThat(authorizationHeaderValue, is(expected));
    }

}
