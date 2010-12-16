package com.akiban.cserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.cserver.manage.ManageMXBean;
import com.akiban.cserver.manage.ManageMXBeanImpl;
import com.akiban.cserver.service.DefaultServiceManagerFactory;
import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.ServiceManagerFactory;
import com.akiban.cserver.service.jmx.JmxManageable;
import com.akiban.util.Tap;

/**
 * @author peter
 */
public class CServer implements CServerConstants, Service<CServer>, JmxManageable {

    private static final Log LOG = LogFactory.getLog(CServer.class.getName());

    /**
     * Config property name and default for the port on which the CServer will
     * listen for requests.
     *
     * /** Port on which the CServer will listen for requests.
     */
    private static final int CSERVER_PORT = Integer.parseInt(System.getProperty(
            "cserver.port", DEFAULT_CSERVER_PORT_STRING));

    /**
     * Interface on which this cserver instance will listen. TODO - allow
     * multiple NICs
     */

    private static final String CSERVER_HOST = System.getProperty(
            "cserver.host", DEFAULT_CSERVER_HOST_STRING);

    private static final boolean TCP_NO_DELAY =
        Boolean.parseBoolean(System.getProperty("com.akiban.server.tcpNoDelay", "true"));

    /**
     * Name of this chunkserver. Must match one of the entries in
     * /config/cluster.properties (managed by Admin).
     */
    private static final String CSERVER_NAME = System.getProperty("cserver.name");

    private final ServiceManager serviceManager;
    
    private final int cserverPort = CSERVER_PORT; // TODO - get from
                                                  // ConfigurationService

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
    public void stop() throws Exception
    {
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

    public ServiceManager getServiceManager()
    {
        return serviceManager;
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return jmxObjectInfo;
    }


    @Override
    public CServer cast() {
        return this;
    }

    @Override
    public Class<CServer> castClass() {
        return CServer.class;
    }

    /**
     * @param args
     *            the command line arguments
     */
    public static void main(String[] args) throws Exception
    {
        final ServiceManagerFactory serviceManagerFactory = new DefaultServiceManagerFactory();
        final ServiceManager serviceManager = serviceManagerFactory.serviceManager();
        serviceManager.startServices();
    }
}
