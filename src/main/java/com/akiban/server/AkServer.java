/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server;

import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.store.Store;
import com.akiban.util.OsUtils;
import com.akiban.util.Strings;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.error.TapBeanFailureException;
import com.akiban.server.manage.ManageMXBean;
import com.akiban.server.manage.ManageMXBeanImpl;
import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.util.tap.Tap;

import javax.management.ObjectName;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;

/**
 * @author peter
 */
public class AkServer implements Service, JmxManageable, AkServerInterface
{
    private static final String VERSION_STRING_FILE = "version/akserver_version";
    public static final String VERSION_STRING = getVersionString();

    private static final Logger LOG = LoggerFactory.getLogger(AkServer.class.getName());
    private static final String AKSERVER_NAME = System.getProperty("akserver.name", "Akiban Server");
    private static final String PID_FILE_NAME = System.getProperty("akserver.pidfile");

    private final JmxObjectInfo jmxObjectInfo;

    @Inject
    public AkServer(Store store, DXLService dxl, SessionService sessionService) {
        this.jmxObjectInfo = new JmxObjectInfo(
                "AKSERVER",
                new ManageMXBeanImpl(store, dxl, sessionService),
                ManageMXBean.class
        );
    }

    @Override
    public void start() {
        try {
            Tap.registerMXBean();
        } catch (Exception e) {
            throw new TapBeanFailureException (e.getMessage());
        }
    }

    @Override
    public void stop() 
    {
        try {
            Tap.unregisterMXBean();
        } catch (Exception e) {
            throw new TapBeanFailureException(e.getMessage());
        }
    }
    
    @Override
    public void crash() {
        stop();
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return jmxObjectInfo;
    }

    @Override
    public String getServerName()
    {
        return AKSERVER_NAME;
    }

    @Override
    public String getServerVersion()
    {
        return VERSION_STRING;
    }

    private static String getVersionString()
    {
        try {
            return Strings.join(Strings.dumpResource(null,
                    VERSION_STRING_FILE));
        } catch (IOException e) {
            LOG.warn("Couldn't read resource file");
            return "Error: " + e;
        }
    }

    public interface ShutdownMXBean {
        public void shutdown();
    }

    private static class ShutdownMXBeanImpl implements ShutdownMXBean {
        private static final String BEAN_NAME = "com.akiban:type=SHUTDOWN";
        private final ServiceManager sm;

        public ShutdownMXBeanImpl(ServiceManager sm) {
            this.sm = sm;
        }

        @Override
        public void shutdown() {
            try {
                if(sm != null) {
                    sm.stopServices();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        GuicedServiceManager.BindingsConfigurationProvider bindingsConfigurationProvider = GuicedServiceManager.standardUrls();
        ServiceManager serviceManager = new GuicedServiceManager(bindingsConfigurationProvider);

        final ShutdownMXBeanImpl shutdownBean = new ShutdownMXBeanImpl(serviceManager);
        

        // JVM shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                shutdownBean.shutdown();
            }
        }, "ShutdownHook"));

        // Bring system up
        serviceManager.startServices();
        
        // JMX shutdown method
        try {
            ObjectName name = new ObjectName(ShutdownMXBeanImpl.BEAN_NAME);
            ManagementFactory.getPlatformMBeanServer().registerMBean(shutdownBean, name);
        } catch(Exception e) {
            LOG.error("Exception registering shutdown bean", e);
        }
        
        
        // services started successfully, now create pidfile and write pid to it
        if (PID_FILE_NAME != null) {
            File pidFile = new File(PID_FILE_NAME);
            pidFile.deleteOnExit();
            FileWriter out = new FileWriter(pidFile);
            out.write(OsUtils.getProcessID());
            out.flush();
        }
    }

    /** Start from procrun.
     * @see <a href="http://commons.apache.org/daemon/procrun.html">Daemon: Procrun</a>
     */
    public static void procrunStart(String[] args) throws Exception {
        // Start server and return from this thread.
        // Normal entry does that.
        main(args);
    }

    public static void procrunStop(String[] args) throws Exception {
        // Stop server from another thread.
        // Need global access to ServiceManager. Can get it via the shutdown bean.
        ObjectName name = new ObjectName(ShutdownMXBeanImpl.BEAN_NAME);
        ManagementFactory.getPlatformMBeanServer().invoke(name, "shutdown",
                                                          new Object[0], new String[0]);
    }

}
