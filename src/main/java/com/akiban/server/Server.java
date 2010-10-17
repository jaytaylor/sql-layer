package com.akiban.server;

import com.akiban.message.ExecutionContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Server extends Thread
{
    // Thread interface

    public void run()
    {
        synchronized (this) {
            state = State.RUNNING;
            notifyAll();
        }
        try {
            while (state == State.RUNNING) {
                Socket clientSocket = serverSocket.accept();
                clientSocket.setTcpNoDelay(TCP_NO_DELAY);
                startServicingClient(clientSocket);
            }
        } catch (SocketException e) {
            LOG.info("Server shutting down normally");
            termination = e;
        } catch (Exception e) {
            LOG.error("Caught exception while waiting for client connection", e);
            termination = e;
        }
    }

    // Server interface

    public static Server startServer(String label,
                                     String host,
                                     int port,
                                     ExecutionContext executionContext,
                                     RequestHandler requestHandler) throws InterruptedException, IOException
    {
        Server server = new Server(label, host, port, executionContext, requestHandler);
        server.setName(String.format("Server:%s", port));
        server.setDaemon(true);
        server.start();
        synchronized (server) {
            while (server.state == State.STARTING) {
                server.wait();
            }
        }
        return server;
    }

    public void stopServer() throws IOException, InterruptedException
    {
        synchronized (this) {
            state = State.STOPPED;
            notifyAll();
        }
        if (serverSocket != null) {
            serverSocket.close();
            Thread.sleep(1000); // Address is apparently not guaranteed to be released on return from close.
        }
        shutdown();
    }

    public synchronized boolean stopped()
    {
        return state == State.STOPPED;
    }

    public Exception termination()
    {
        return termination;
    }

    /**
     * Called after a connection to a client has been established and before any requests have been served.
     *
     * @param requestRunner
     */
    public void onConnect(RequestRunner requestRunner)
    {
    }

    /**
     * Called after a connection to a client has been closed or the thread servicing catches an exception,
     * (and after onException has been called). The connection will be closed by the server, but inside onDisconnect,
     * the state of the connection is not reliable.
     *
     * @param requestRunner
     */
    public void onDisconnect(RequestRunner requestRunner)
    {
    }

    /**
     * Called after an exception (other than SocketException, which indicates a normal connection close).
     *
     * @param requestRunner
     */
    public void onException(RequestRunner requestRunner)
    {
    }

    // For use by this class

    private void startServicingClient(Socket clientSocket) throws IOException
    {
        RequestRunner requestRunner = new RequestRunner(this, clientSocket, executionContext, requestHandler);
        onConnect(requestRunner);
        threadPool.execute(requestRunner);
    }

    private void shutdown()
    {
        // This code is from JDK 1.6 ExecutorService docs
        threadPool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!threadPool.awaitTermination(WAIT_FOR_TERMINATION_SEC, TimeUnit.SECONDS)) {
                LOG.info("Gentle shutdown didn't work. Trying harder.");
                threadPool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!threadPool.awaitTermination(WAIT_FOR_TERMINATION_SEC, TimeUnit.SECONDS)) {
                    LOG.error("Server thread pool did not terminate.");
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            threadPool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    private Server(final String label,
                   String host,
                   int port,
                   ExecutionContext executionContext,
                   RequestHandler requestHandler) throws IOException
    {
        this.serverSocket = new ServerSocket(port, MAX_PENDING_REQUESTS, Inet4Address.getByName(host));
        this.executionContext = executionContext;
        this.requestHandler = requestHandler;
        threadPool = Executors.newCachedThreadPool(
            new ThreadFactory()
            {
                @Override
                public Thread newThread(Runnable r)
                {
                    Thread thread = new Thread(r);
                    thread.setName(String.format("%s-%s", label, threadIdGenerator.getAndIncrement()));
                    thread.setDaemon(true);
                    return thread;
                }
            });
    }

    // Class state

    private static final Log LOG = LogFactory.getLog(Server.class);
    private static final int MAX_PENDING_REQUESTS = 100;
    private static final int WAIT_FOR_TERMINATION_SEC = 10;
    private static final AtomicInteger threadIdGenerator = new AtomicInteger(0);
    private static final boolean TCP_NO_DELAY =
        Boolean.parseBoolean(System.getProperty("com.akiban.server.tcpNoDelay", "true"));

    // Object state

    private final ServerSocket serverSocket;
    private final ExecutionContext executionContext;
    private final RequestHandler requestHandler;
    private final ExecutorService threadPool;
    private Exception termination;
    private volatile State state = State.STARTING;

    // Inner classes

    private enum State
    {
        STARTING, RUNNING, STOPPED
    }
}
