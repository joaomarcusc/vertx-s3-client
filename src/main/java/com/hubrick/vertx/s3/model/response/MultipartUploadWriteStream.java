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
package com.hubrick.vertx.s3.model.response;

import com.hubrick.vertx.s3.client.S3Client;
import com.hubrick.vertx.s3.model.Part;
import com.hubrick.vertx.s3.model.request.AbortMultipartUploadRequest;
import com.hubrick.vertx.s3.model.request.CompleteMultipartUploadRequest;
import com.hubrick.vertx.s3.model.request.ContinueMultipartUploadRequest;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Emir Dizdarevic
 * @since 3.0.0
 */
public class MultipartUploadWriteStream implements WriteStream<Buffer> {

    private static final Integer FIVE_MB_IN_BYTES = 5242880;
    private static final Integer DEFAULT_MAX_OUTSTANDING_BUFFER_WRITES = 20;

    private final S3Client s3Client;
    private final InitMultipartUploadResponse initMultipartUploadResponse;

    private Handler<Void> endHandler;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> drainHandler;

    private final Map<Integer, String> partETagMap = new TreeMap<>();
    private Integer nextPartNumber = 1;
    private Integer outstandingBufferWrites = 0;
    private Integer maxOutstandingBufferWrites = DEFAULT_MAX_OUTSTANDING_BUFFER_WRITES;
    private boolean endCalled = false;
    private boolean abortOnFailure = true;
    private boolean aborted = false;
    private Integer bufferSize = FIVE_MB_IN_BYTES;
    private Buffer buffer = Buffer.buffer(bufferSize);

    public MultipartUploadWriteStream(S3Client s3Client, InitMultipartUploadResponse initMultipartUploadResponse, Handler<Throwable> exceptionHandler) {
        checkNotNull(s3Client, "s3Client must not be null");
        checkNotNull(initMultipartUploadResponse, "initMultipartUploadResponse must not be null");
        checkNotNull(exceptionHandler, "exceptionHandler must not be null");

        this.s3Client = s3Client;
        this.initMultipartUploadResponse = initMultipartUploadResponse;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public WriteStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    /**
     * The endHandler which will be called after either end() was called.
     * It waits until the response from S3 was received.
     *
     * @param endHandler The endHandler was called.
     * @return This
     */
    public WriteStream<Buffer> endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        return this;
    }

    public WriteStream<Buffer> bufferSize(Integer bufferSize) {
        checkNotNull(bufferSize, "bufferSize must not be null");
        checkArgument(bufferSize >= FIVE_MB_IN_BYTES, "bufferSize minimum size must be 5Mb");

        this.bufferSize = bufferSize;
        return this;
    }

    public WriteStream<Buffer> abortOnFailure(boolean abortOnFailure) {
        this.abortOnFailure = abortOnFailure;
        return this;
    }

    @Override
    public WriteStream<Buffer> write(Buffer data) {
        buffer.appendBuffer(data);
        if (buffer.length() >= bufferSize || endCalled) {
            final Integer currentPartNumber = nextPartNumber++;
            final Buffer currentBuffer = buffer;
            this.buffer = Buffer.buffer(bufferSize);
            outstandingBufferWrites++;
            s3Client.continueMultipartUpload(
                    initMultipartUploadResponse.getBucket(),
                    initMultipartUploadResponse.getKey(),
                    new ContinueMultipartUploadRequest(currentBuffer, currentPartNumber, initMultipartUploadResponse.getUploadId()),
                    response -> {
                        // Save nextPartNumber together with ETag required for the complete operation
                        partETagMap.put(currentPartNumber, response.getHeader().getETag());
                        decreaseOutstandingBufferWrites();
                        endIfAllPartsAreUploaded();
                    },
                    throwable -> {
                        if (abortOnFailure) {
                            if (!aborted) {
                                abort(aVoid -> exceptionHandler.handle(throwable));
                            }
                        } else {
                            exceptionHandler.handle(throwable);
                        }
                    }
            );
        }
        return this;
    }

    @Override
    public void end() {
        endCalled = true;
        if (buffer.length() > 0) {
            write(Buffer.buffer());
        }
        endIfAllPartsAreUploaded();
    }

    public void end(Handler<Void> endHandler) {
        endHandler(endHandler);
        end();
    }

    private void endIfAllPartsAreUploaded() {
        if (endCalled && partETagMap.size() == nextPartNumber - 1) {
            s3Client.completeMultipartUpload(
                    initMultipartUploadResponse.getBucket(),
                    initMultipartUploadResponse.getKey(),
                    new CompleteMultipartUploadRequest(
                            initMultipartUploadResponse.getUploadId(),
                            partETagMap.entrySet().stream().map(e -> new Part(e.getKey(), e.getValue())).collect(Collectors.toList())
                    ),
                    response -> {
                        if (endHandler != null) {
                            endHandler.handle(null);
                        }
                    },
                    exceptionHandler
            );
        }
    }

    public void abort(Handler<Void> handler) {
        s3Client.abortMultipartUpload(
                initMultipartUploadResponse.getBucket(),
                initMultipartUploadResponse.getKey(),
                new AbortMultipartUploadRequest(
                        initMultipartUploadResponse.getUploadId()
                ),
                response -> handler.handle(null),
                exceptionHandler
        );
        aborted = true;
    }

    private void decreaseOutstandingBufferWrites() {
        outstandingBufferWrites--;
        if (outstandingBufferWrites <= maxOutstandingBufferWrites / 2 && drainHandler != null) {
            try {
                drainHandler.handle(null);
            } catch (Throwable t) {
                if (exceptionHandler != null) {
                    exceptionHandler.handle(t);
                }
            }
        }
    }

    @Override
    public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
        checkArgument(maxSize >= 2, "maxSize must be bigger then 2");

        maxOutstandingBufferWrites = maxSize;
        return this;
    }

    @Override
    public boolean writeQueueFull() {
        return outstandingBufferWrites > maxOutstandingBufferWrites;
    }

    @Override
    public WriteStream<Buffer> drainHandler(Handler<Void> handler) {
        this.drainHandler = handler;
        return this;
    }

    public InitMultipartUploadResponse getInitMultipartUploadResponse() {
        return initMultipartUploadResponse;
    }
}
