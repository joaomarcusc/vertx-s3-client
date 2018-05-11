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
package com.hubrick.vertx.s3.util;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.LinkedList;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Emir Dizdarevic
 * @since 3.1.0
 */
public class ChunkedBufferReadStream implements ReadStream<Buffer> {

    private static final Logger log = LoggerFactory.getLogger(ChunkedBufferReadStream.class);

    private final Vertx vertx;
    private final ReadStream<Buffer> readStream;
    private final int minChunkSize;
    private final Deque<Buffer> lastChunks = new LinkedList<>();

    private boolean paused;
    private boolean endCalled;
    private boolean endHandled;
    private Handler<Buffer> handler;
    private Handler<Void> endHandler;
    private Handler<Throwable> exceptionHandler;
    private Handler<Buffer> chunkHandler;
    private Buffer buffer;
    private int chunks = 0;

    public ChunkedBufferReadStream(Vertx vertx, ReadStream<Buffer> readStream, int minChunkSize) {
        checkNotNull(vertx, "vertx must not be null");
        checkNotNull(readStream, "readStream must not be null");

        this.vertx = vertx;
        this.readStream = readStream;
        this.minChunkSize = minChunkSize;
        init();
    }

    private void init() {
        buffer = Buffer.buffer(minChunkSize);
        readStream.handler(data -> {
            buffer.appendBuffer(data);
            handleChunk();
        });
        readStream.endHandler(event -> {
            endCalled = true;
            handleChunk();
        });
        readStream.exceptionHandler(throwable -> {
            if (exceptionHandler != null) {
                exceptionHandler.handle(throwable);
            }
        });
    }

    private void handleChunk() {
        try {
            boolean pushed = false;

            // Check chunks to make sure that the handler is called at least once.
            if (buffer.length() >= minChunkSize || (endCalled && (buffer.length() > 0 || chunks == 0))) {
                pushed = true;
                if (chunkHandler != null) {
                    chunkHandler.handle(buffer.slice());
                    pushBuffer();
                } else {
                    pushBuffer();
                }
                incChunks();
            }

            if (pushed || endCalled) {
                writeChunkAsync(false);
            }
        } catch (Throwable t) {
            if (exceptionHandler != null) {
                exceptionHandler.handle(t);
            }
        }
    }

    private void pushBuffer() {
        lastChunks.add(buffer);
        buffer = Buffer.buffer(minChunkSize);
    }

    private void writeChunkAsync(boolean resumed) {
        vertx.runOnContext(aVoid -> {
            try {
                writeChunk(resumed);
            } catch (Throwable t) {
                if (exceptionHandler != null) {
                    exceptionHandler.handle(t);
                }
            }
        });
    }

    private void writeChunk(boolean resumed) {
        if (!paused) {
            try {
                while (!lastChunks.isEmpty()) {
                    final Buffer currentChunk = lastChunks.pop();
                    if (currentChunk.length() > 0) {
                        if (handler != null) {
                            handler.handle(currentChunk.slice());
                        }
                    }
                }

                if (endCalled && !endHandled) {
                    endHandled = true;
                    if (endHandler != null) {
                        endHandler.handle(null);
                    }
                }
            } catch (Throwable t) {
                pause();
                if (exceptionHandler != null) {
                    exceptionHandler.handle(t);
                }
            }
        } else if (resumed) {
            writeChunkAsync(resumed);
        }
    }

    /**
     * Will be called with every chunk to verify if the chunk is valid.
     * The method should return true if the pump should continue. Otherwise it will be stopped.
     *
     * @param chunkHandler The chunk handler
     * @return This instance
     */
    public ChunkedBufferReadStream setChunkHandler(Handler<Buffer> chunkHandler) {
        this.chunkHandler = chunkHandler;
        return this;
    }


    @Override
    public ChunkedBufferReadStream exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler = handler;
        return this;
    }

    @Override
    public ChunkedBufferReadStream handler(Handler<Buffer> handler) {
        this.handler = handler;
        return this;
    }

    @Override
    public ChunkedBufferReadStream pause() {
        this.paused = true;
        this.readStream.pause();
        return this;
    }

    @Override
    public ChunkedBufferReadStream resume() {
        this.paused = false;
        this.readStream.resume();
        writeChunkAsync(true);
        return this;
    }

    @Override
    public ChunkedBufferReadStream endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        return this;
    }

    /**
     * Return the total number of elements pumped by this pump.
     */
    public synchronized int numberOfChunks() {
        return chunks;
    }

    // Note we synchronize as numberPumped can be called from a different thread however incPumped will always
    // be called from the same thread so we benefit from bias locked optimisation which should give a very low
    // overhead
    private synchronized void incChunks() {
        chunks++;
    }
}
