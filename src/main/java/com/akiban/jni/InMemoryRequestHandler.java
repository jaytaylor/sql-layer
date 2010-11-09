package com.akiban.jni;

import com.akiban.ais.message.CServerContext;
import com.akiban.cserver.AbstractCServerRequestHandler;
import com.akiban.message.AkibanConnection;
import com.akiban.message.Message;
import com.akiban.message.Request;
import org.jboss.netty.util.internal.ConcurrentHashMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryRequestHandler extends AbstractCServerRequestHandler {
    @Override
    public void start() throws IOException, InterruptedException {
    }

    @Override
    public void stop() throws IOException, InterruptedException {
    }

    @Override
    public void handleRequest(AkibanConnection connection, Request request)
            throws Exception
    {
        ((CServerContext)executionContext).executeRequest(connection, request);
    }

    final ConcurrentHashMap<Long,ExecutionRunnable> runnables = new ConcurrentHashMap<Long, ExecutionRunnable>();
    final AtomicLong handlersCount = new AtomicLong();
    /**
     * Opens a connection and returns a handle that uniquely identifies it.
     * @return a handle to be used in subsequent method invocations.
     */
    public long openConnection() {
        final long handler = handlersCount.incrementAndGet();
        final ExecutionRunnable runnable = new ExecutionRunnable();
        runnables.put(handler, runnable);
        runnable.start();
        return handler;
    }

    public boolean closeConnection(long handle) {
        final ExecutionRunnable runnable = runnables.remove(handle);
        if (runnable == null) {
            return false;
        }
        try {
            runnable.interrupt();
            return true;
        } catch (SecurityException e) {
            return false;
        }
    }

    public boolean putRequest(long handle, ByteBuffer requestPayload) {
        return putRequest(handle, requestPayload, null);
    }

    public boolean putRequest(long handle, ByteBuffer requestPayload, ByteBuffer responsePayload) {
        final ExecutionRunnable runnable = runnables.get(handle);
        if (runnable == null) {
            return false;
        }
        if (responsePayload != null) {
            runnable.connection.addToOutputPool(responsePayload);
        }
        try {
            runnable.connection.putToReceive(requestPayload);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public ByteBuffer getResponse(long handle) {
        final ExecutionRunnable runnable = runnables.get(handle);
        if (runnable == null) {
            return null;
        }
        try {
            return runnable.connection.getFromSent();
        }
        catch (Exception e) {
            return null;
        }
    }

    private class ExecutionRunnable extends Thread {
        private final InMemoryAkibanConnection connection;

        ExecutionRunnable() {
            super();
            this.connection = new InMemoryAkibanConnection();
        }

        @Override
        public void run() {
            while(true) {
                try {
                    Message request = connection.receive();
                    handleRequest(connection, ((Request)request));
                }
                catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }
}
