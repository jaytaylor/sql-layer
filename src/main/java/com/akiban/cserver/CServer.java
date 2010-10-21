package com.akiban.cserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.admin.Admin;
import com.akiban.ais.message.AISExecutionContext;
import com.akiban.ais.message.AISRequest;
import com.akiban.ais.message.AISResponse;
import com.akiban.cserver.manage.SchemaManager;
import com.akiban.cserver.message.ShutdownRequest;
import com.akiban.cserver.message.ShutdownResponse;
import com.akiban.cserver.message.ToStringWithRowDefCache;
import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.store.Store;
import com.akiban.message.AkibaSendConnection;
import com.akiban.message.AkibanConnection;
import com.akiban.message.ErrorCode;
import com.akiban.message.ErrorResponse;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Message;
import com.akiban.message.MessageRegistry;
import com.akiban.message.Request;
import com.akiban.util.Tap;

/**
 * @author peter
 */
public class CServer implements CServerConstants, Service {

    private static final Log LOG = LogFactory.getLog(CServer.class.getName());

    /**
     * Config property name and default for the port on which the CServer will
     * listen for requests.
     * 
     * /** Port on which the CServer will listen for requests.
     */
    public static final int CSERVER_PORT = Integer.parseInt(System.getProperty(
            "cserver.port", DEFAULT_CSERVER_PORT_STRING));

    /**
     * Interface on which this cserver instance will listen. TODO - allow
     * multiple NICs
     */

    public static final String CSERVER_HOST = System.getProperty(
            "cserver.host", DEFAULT_CSERVER_HOST_STRING);

    private static final boolean USE_NETTY = Boolean.parseBoolean(System
            .getProperty("usenetty", "false"));

    /**
     * Name of this chunkserver. Must match one of the entries in
     * /config/cluster.properties (managed by Admin).
     */
    private static final String CSERVER_NAME = System
            .getProperty("cserver.name");

    private static Tap CSERVER_EXEC = Tap.add(new Tap.PerThread("cserver",
            Tap.TimeStampLog.class));

    private final ServiceManagerImpl serviceManager;
    
    private CServerConfig config; // TODO - remove

    private final int cserverPort = CSERVER_PORT; // TODO - get from
                                                  // ConfigurationService

    private final ExecutionContext executionContext = new CServerContext();
    private AbstractCServerRequestHandler requestHandler;
    private volatile boolean stopped;
    private boolean leadCServer;
    private AISDistributor aisDistributor;

    private volatile Thread _shutdownHook;
    
    public CServer(final ServiceManagerImpl serviceManager) {
        this.serviceManager = serviceManager;
    }

    @Override
    public void start() throws Exception {
        start(true);
    }

    public void start(boolean startNetwork) throws Exception {
        // TODO - remove static reference
        Store store = serviceManager.getStore(); // TODO inject
                                                           // service manager

        LOG.warn(String.format("Starting chunkserver %s on port %s",
                CSERVER_NAME, CSERVER_PORT));
        Tap.registerMXBean();

        if (startNetwork) {
            MessageRegistry.initialize();
            MessageRegistry.only().registerModule("com.akiban.cserver");
            MessageRegistry.only().registerModule("com.akiban.ais");
            MessageRegistry.only().registerModule("com.akiban.message");
        }

        if (false) {
            Admin admin = Admin.only();

            leadCServer = admin.clusterConfig().leadChunkserver().name()
                    .equals(CSERVER_NAME);
            admin.markChunkserverUp(CSERVER_NAME);
            if (isLeader()) {
                aisDistributor = new AISDistributor(this);
            }
        } else {
            leadCServer = true;
        }
        LOG.warn(String.format("Started chunkserver %s on port %s, lead = %s",
                CSERVER_NAME, CSERVER_PORT, isLeader()));
        _shutdownHook = new Thread(new Runnable() {
            public void run() {
                try {
                    serviceManager.stopServices();
                } catch (Exception e) {

                }
            }
        }, "ShutdownHook");

        Runtime.getRuntime().addShutdownHook(_shutdownHook);

        if (startNetwork) {
            requestHandler = USE_NETTY ? CServerRequestHandler_Netty.start(
                    this, CSERVER_HOST, CSERVER_PORT) : CServerRequestHandler
                    .start(this, CSERVER_HOST, CSERVER_PORT);
        }
    }

    @Override
    public void stop() throws Exception {
        stopped = true;
        final Thread hook = _shutdownHook;
        _shutdownHook = null;
        if (hook != null) {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
        if (requestHandler != null) {
            requestHandler.stop();
        } // else: testing - chunkserver started without network
        if (false) {
            // TODO: Use this when we support multiple chunkservers
            Admin.only().markChunkserverDown(CSERVER_NAME);
        }

        Tap.unregisterMXBean();
    }

    boolean isLeader() {
        return leadCServer;
    }

    public String host() {
        return CSERVER_HOST;
    }

    public int port() {
        return cserverPort;
    }

    public Store getStore() {
        return serviceManager.getStore();
    }

    public class CServerContext implements ExecutionContext,
            AISExecutionContext, CServerShutdownExecutionContext {

        public SchemaManager getSchemaManager() {
            return getStore().getSchemaManager();
        }
        
        // TODO - temporarily this simplifies the execute methods
        // in the Request messages.  Remove this when dispatch
        // changes.
        public Store getStore() {
            return CServer.this.getStore();
        }

        @Override
        public void executeRequest(AkibaSendConnection connection,
                AISRequest request) throws Exception {
            AISResponse aisResponse = new AISResponse(getStore()
                    .getAis());
            connection.send(aisResponse);
        }

        @Override
        public void executeResponse(AkibaSendConnection connection,
                AISResponse response) throws Exception {
            // TODO - remove this entirely - waiting for new request
            // dispatch mechanism.
        }

        @Override
        public void executeRequest(AkibanConnection connection,
                ShutdownRequest request) throws Exception {
            if (LOG.isInfoEnabled()) {
                LOG.info("CServer stopping due to ShutdownRequest");
            }
            ShutdownResponse response = new ShutdownResponse();
            connection.send(response);
            serviceManager.stopServices();
        }
    }

    Runnable newRunnable(AkibanConnection connection) {
        return new CServerRunnable(connection);
    }

    ExecutionContext executionContext() {
        return executionContext;
    }

    Message executeMessage(Message request) {
        if (stopped) {
            return new ErrorResponse(ErrorCode.SERVER_SHUTDOWN,
                    "Server is shutting down");
        }

        final SingleSendBuffer sendBuffer = new SingleSendBuffer();
        try {
            request.execute(sendBuffer, executionContext);
        } catch (InvalidOperationException e) {
            sendBuffer.send(new ErrorResponse(e.getCode(), e.getMessage()));
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Message type %s generated an error",
                        request.getClass()), e);
            }
        } catch (Throwable t) {
            sendBuffer.send(new ErrorResponse(t));
        }
        return sendBuffer.getMessage();
    }

    void executeRequest(ExecutionContext executionContext,
            AkibanConnection connection, Request request) throws Exception {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Serving message " + request);
        }
        CSERVER_EXEC.in();
        if (getStore().isVerbose() && LOG.isInfoEnabled()) {
            LOG.info(String
                    .format("Executing %s",
                            request instanceof ToStringWithRowDefCache ? ((ToStringWithRowDefCache) request)
                                    .toString(getStore()
                                            .getRowDefCache()) : request
                                    .toString()));
        }
        Message response = executeMessage(request);
        connection.send(response);
        CSERVER_EXEC.out();
    }

    /**
     * A Runnable that reads Network messages, acts on them and returns results.
     * 
     * @author peter
     * 
     */
    private class CServerRunnable implements Runnable {

        private final AkibanConnection connection;

        public CServerRunnable(final AkibanConnection connection) {
            this.connection = connection;
        }

        public void run() {
            Message message = null;
            while (!stopped) {
                try {
                    message = connection.receive();
                    executeRequest(executionContext, connection,
                            (Request) message);
                } catch (InterruptedException e) {
                    if (LOG.isInfoEnabled()) {
                        LOG.info("Thread " + Thread.currentThread().getName()
                                + (stopped ? " stopped" : " interrupted"));
                    }
                    break;
                } catch (Exception e) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error("Unexpected error on " + message, e);
                    }
                    if (message != null) {
                        try {
                            connection.send(new ErrorResponse(e));
                        } catch (Exception f) {
                            LOG.error("Caught " + f.getClass()
                                    + " while sending error response to "
                                    + message + ": " + f.getMessage(), f);
                        }
                    }
                }
            }
        }
    }

    /**
     * @param args
     *            the command line arguments
     */
    public static void main(String[] args) throws Exception {
        try {
            final ServiceManagerImpl serviceManager = new ServiceManagerImpl();
            serviceManager.setupCServerConfig(); // TODO - temporary
            serviceManager.startServices();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

    }

}
