package com.akiban.cserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.ais.message.CServerContext;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.manage.ManageMXBean;
import com.akiban.cserver.manage.ManageMXBeanImpl;
import com.akiban.cserver.message.ToStringWithRowDefCache;
import com.akiban.cserver.service.DefaultServiceManagerFactory;
import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.ServiceManagerFactory;
import com.akiban.cserver.service.jmx.JmxManageable;
import com.akiban.cserver.store.Store;
import com.akiban.message.AkibanConnection;
import com.akiban.message.ErrorCode;
import com.akiban.message.ErrorResponse;
import com.akiban.message.Message;
import com.akiban.message.Request;
import com.akiban.util.Tap;

/**
 * @author peter
 */
public class CServer implements CServerConstants, CServerContext, Service, JmxManageable {

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

    private static final boolean TCP_NO_DELAY =
        Boolean.parseBoolean(System.getProperty("com.akiban.server.tcpNoDelay", "true"));

    /**
     * Name of this chunkserver. Must match one of the entries in
     * /config/cluster.properties (managed by Admin).
     */
    private static final String CSERVER_NAME = System
            .getProperty("cserver.name");

    private static Tap CSERVER_EXEC = Tap.add(new Tap.PerThread("cserver",
            Tap.TimeStampLog.class));

    private final ServiceManager serviceManager;
    
    private final int cserverPort = CSERVER_PORT; // TODO - get from
                                                  // ConfigurationService

    private volatile boolean stopped;

    private volatile Thread _shutdownHook;
    
    private final JmxObjectInfo jmxObjectInfo;

    public CServer(final ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
        this.jmxObjectInfo = new JmxObjectInfo("CSERVER", new ManageMXBeanImpl(
                this), ManageMXBean.class);
    }


    @Override
    public void start() throws Exception {
        LOG.warn(String.format("Starting chunkserver %s on port %s",
                CSERVER_NAME, CSERVER_PORT));
        Tap.registerMXBean();
        LOG.warn(String.format("Started chunkserver %s on port %s", CSERVER_NAME, CSERVER_PORT));
        _shutdownHook = new Thread(new Runnable() {
            public void run() {
                try {
                    serviceManager.stopServices();
                } catch (Exception e) {
                    LOG.warn("Caught exception while stopping services", e);
                }
            }
        }, "ShutdownHook");
        Runtime.getRuntime().addShutdownHook(_shutdownHook);
    }

    @Override
    public void stop() throws Exception {
        stopped = true;
        final Thread hook = _shutdownHook;
        _shutdownHook = null;
        if (hook != null) {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
        Tap.unregisterMXBean();
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

    @Override
    public AkibaInformationSchema ais() {
        return getStore().getAis();
    }
    
    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return jmxObjectInfo;
    }

    private Message executeMessage(Message request) {
        if (stopped) {
            return new ErrorResponse(ErrorCode.SERVER_SHUTDOWN,
                    "Server is shutting down");
        }

        final SingleSendBuffer sendBuffer = new SingleSendBuffer();
        try {
            request.execute(sendBuffer, this);
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

    public void executeRequest(AkibanConnection connection, Request request) throws Exception {
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
     * @param args
     *            the command line arguments
     */
    public static void main(String[] args) throws Exception {
        final ServiceManagerFactory serviceManagerFactory = new DefaultServiceManagerFactory();
        final ServiceManager serviceManager = serviceManagerFactory.serviceManager();
        serviceManager.startServices();
    }
}
