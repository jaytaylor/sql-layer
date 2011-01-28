/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.jni;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.akiban.message.AkibanConnection;
import com.akiban.message.Message;
import com.akiban.message.MessageRegistry;

public final class InMemoryAkibanConnection implements AkibanConnection {
    private static final int MESSAGE_BUFFER_SIZE = 1 << 20;

    private final BlockingQueue<ByteBuffer> requests = new LinkedBlockingQueue<ByteBuffer>();
    private final BlockingQueue<ByteBuffer> responses = new LinkedBlockingQueue<ByteBuffer>();
    private final Queue<ByteBuffer> availableOutBuffers = new ArrayDeque<ByteBuffer>();
    private final Object MONITOR = availableOutBuffers;

    @Override
    public Message receive() throws Exception {
        ByteBuffer request = requests.take();
        short type = request.getShort();
        final Message message = MessageRegistry.only().newInstance(type);
        message.read(request);
        return message;
    }

    @Override
    public Message sendAndReceive(Message request) throws Exception {
        send(request);
        return receive();
    }

    @Override
    public void close() {
    }

    @Override
    public void send(Message message) throws Exception {
        ByteBuffer out;
        synchronized (MONITOR) {
            out = availableOutBuffers.poll();
        }
        if (out == null) {
            out = ByteBuffer.allocateDirect(MESSAGE_BUFFER_SIZE);
        }

        message.write(out);
        responses.put(out);
    }

    void addToOutputPool(ByteBuffer output) {
        if (output == null) {
            throw new IllegalArgumentException("arg can't be null");
        }
        synchronized (MONITOR) {
            availableOutBuffers.add(output);
        }
    }

    /**
     * Puts a message onto the "receives" queue; it will be available to callers of {@linkplain #receive()}
     * @param messageToReceive the message to put on the queue
     * @throws InterruptedException if interrupted
     */
    void putToReceive(ByteBuffer messageToReceive) throws InterruptedException {
        requests.put(messageToReceive);
    }

    /**
     * Gets a message from the sent queue; this message will have been passed to {@linkplain #send(Message)}
     * @return a message from the sent queue
     * @throws InterruptedException if interrupted
     */
    ByteBuffer getFromSent() throws InterruptedException {
        return responses.take();
    }
}

