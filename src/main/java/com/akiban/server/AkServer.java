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

import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.store.Store;
import com.akiban.util.GCMonitor;
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
    private static final Logger LOG = LoggerFactory.getLogger(AkServer.class.getName());

    private static final String VERSION_STRING_FILE = "version/akserver_version";
    public static final String VERSION_STRING, SHORT_VERSION_STRING;
    public static final int VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH;
    static {
        String vlong, vshort;
        int major = 0, minor = 0, patch = 0;
        try {
            vlong = Strings.join(Strings.dumpResource(null, VERSION_STRING_FILE));
        } catch (IOException e) {
            LOG.warn("Couldn't read resource file");
            vlong = "Error: " + e;
        }
        int endpos = vlong.indexOf('-');
        if (endpos < 0) endpos = vlong.length();
        vshort = vlong.substring(0, endpos);
        String[] nums = vshort.split("\\.");
        try {
            if (nums.length > 0)
                major = Integer.parseInt(nums[0]);
            if (nums.length > 1)
                minor = Integer.parseInt(nums[1]);
            if (nums.length > 2)
                patch = Integer.parseInt(nums[2]);
        }
        catch (NumberFormatException ex) {
            LOG.warn("Couldn't parse version number: " + vshort);
        }
        VERSION_STRING = vlong;
        SHORT_VERSION_STRING = vshort;
        VERSION_MAJOR = major; 
        VERSION_MINOR = minor; 
        VERSION_PATCH = patch;
    }

    private static final String AKSERVER_NAME_PROP = "akserver.name";
    private static final String GC_INTERVAL_NAME = "akserver.gc_monitor.interval";
    private static final String GC_THRESHOLD_NAME = "akserver.gc_monitor.log_threshold_ms";
    private static final String PID_FILE_NAME = System.getProperty("akserver.pidfile");

    private final JmxObjectInfo jmxObjectInfo;
    private final ConfigurationService config;
    private GCMonitor gcMonitor;

    @Inject
    public AkServer(Store store, DXLService dxl, SessionService sessionService, ConfigurationService config) {
        this.config = config;
        this.jmxObjectInfo = new JmxObjectInfo(
                "AKSERVER",
                new ManageMXBeanImpl(store, dxl, sessionService),
                ManageMXBean.class
        );
    }

    @Override
    public void start() {
        int interval = Integer.parseInt(config.getProperty(GC_INTERVAL_NAME));
        int logThreshold = Integer.parseInt(config.getProperty(GC_THRESHOLD_NAME));
        gcMonitor = new GCMonitor(interval, logThreshold);
        gcMonitor.start();
        try {
            Tap.registerMXBean();
        } catch (Exception e) {
            throw new TapBeanFailureException (e.getMessage());
        }
    }

    @Override
    public void stop() 
    {
        gcMonitor.stopRunning();
        gcMonitor = null;
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
        return config.getProperty(AKSERVER_NAME_PROP);
    }

    @Override
    public String getServerVersion()
    {
        return VERSION_STRING;
    }

    @Override
    public String getServerShortVersion() {
        return SHORT_VERSION_STRING;
    }

    @Override
    public int getServerMajorVersion() {
        return VERSION_MAJOR;
    }

    @Override
    public int getServerMinorVersion() {
        return VERSION_MINOR;
    }

    @Override
    public int getServerPatchVersion() {
        return VERSION_PATCH;
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
    @SuppressWarnings("unused") // Called by procrun
    public static void procrunStart(String[] args) throws Exception {
        // Start server and return from this thread.
        // Normal entry does that.
        main(args);
    }

    @SuppressWarnings("unused") // Called by procrun
    public static void procrunStop(String[] args) throws Exception {
        // Stop server from another thread.
        // Need global access to ServiceManager. Can get it via the shutdown bean.
        ObjectName name = new ObjectName(ShutdownMXBeanImpl.BEAN_NAME);
        ManagementFactory.getPlatformMBeanServer().invoke(name, "shutdown",
                                                          new Object[0], new String[0]);
    }
}
