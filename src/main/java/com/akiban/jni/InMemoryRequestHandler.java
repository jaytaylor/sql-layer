package com.akiban.jni;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.util.internal.ConcurrentHashMap;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.service.network.RequestHandler;
import com.akiban.cserver.service.network.SingleSendBuffer;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.message.AkibanConnection;
import com.akiban.message.ErrorResponse;
import com.akiban.message.Message;
import com.akiban.message.Request;

public class InMemoryRequestHandler extends RequestHandler
{
    // TODO This is duplicated code!
    // I stole it from DefaultRequestHandler. This is definitely a Bad Thing. I am doing it purely
    // to make the svn merge process easier to track. Once the merge is in, we should refactor this duplication
    // away.

    @Override
    public void handleRequest(AkibanConnection connection, Session session, Request request)
        throws Exception
    {
        Message response = executeMessage(request);
        connection.send(response);
    }

    private Message executeMessage(Message request)
    {
        final SingleSendBuffer sendBuffer = new SingleSendBuffer();
        try {
            request.execute(sendBuffer);
        } catch (InvalidOperationException e) {
            sendBuffer.send(new ErrorResponse(e.getCode(), e.getMessage()));
        } catch (Throwable t) {
            sendBuffer.send(new ErrorResponse(t));
        }
        return sendBuffer.getMessage();
    }

    final ConcurrentHashMap<Long, ExecutionRunnable> runnables = new ConcurrentHashMap<Long, ExecutionRunnable>();
    final AtomicLong handlersCount = new AtomicLong();

    /**
     * Opens a connection and returns a handle that uniquely identifies it.
     *
     * @return a handle to be used in subsequent method invocations.
     */
    public long openConnection()
    {
        final long handler = handlersCount.incrementAndGet();
        final ExecutionRunnable runnable = new ExecutionRunnable();
        runnables.put(handler, runnable);
        runnable.start();
        return handler;
    }

    public boolean closeConnection(long handle)
    {
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

    public boolean putRequest(long handle, ByteBuffer requestPayload)
    {
        return putRequest(handle, requestPayload, null);
    }

    public boolean putRequest(long handle, ByteBuffer requestPayload, ByteBuffer responsePayload)
    {
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

    public ByteBuffer getResponse(long handle)
    {
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

    private class ExecutionRunnable extends Thread
    {
        private final InMemoryAkibanConnection connection;
        private final Session session;

        ExecutionRunnable()
        {
            super();
            this.connection = new InMemoryAkibanConnection();
            this.session = new SessionImpl();
        }

        @Override
        public void run()
        {
            while (true) {
                try {
                    Message request = connection.receive();
                    handleRequest(connection, session, ((Request) request));
                }
                catch (InterruptedException e) {
                    break;
                }
                catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }
}
