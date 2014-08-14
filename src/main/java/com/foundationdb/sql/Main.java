/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql;

import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.util.GCMonitor;
import com.foundationdb.util.LoggingStream;
import com.foundationdb.util.OsUtils;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.server.manage.ManageMXBean;
import com.foundationdb.server.manage.ManageMXBeanImpl;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.jmx.JmxManageable;

import javax.management.ObjectName;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Semaphore;

public class Main implements Service, JmxManageable, LayerInfoInterface
{
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final String VERSION_PROPERTY_FILE = "/version/fdbsql_version.properties";
    public static final LayerVersionInfo VERSION_INFO;

    static {
        Properties props = new Properties();
        try(InputStream stream = Main.class.getResourceAsStream(VERSION_PROPERTY_FILE)) {
            props.load(stream);
        } catch (IOException e) {
            LOG.warn("Couldn't read version resource file: {}", VERSION_PROPERTY_FILE);
        }
        VERSION_INFO = new LayerVersionInfo(props);
    }

    private static final String NAME_PROP = "fdbsql.name";
    private static final String GC_INTERVAL_NAME = "fdbsql.gc_monitor.interval";
    private static final String GC_THRESHOLD_NAME = "fdbsql.gc_monitor.log_threshold_ms";
    private static final String PID_FILE_NAME = System.getProperty("fdbsql.pidfile");
    private static final boolean IS_STD_TO_LOG = Boolean.parseBoolean(System.getProperty("fdbsql.std_to_log", "true"));

    private static volatile ShutdownMXBeanImpl shutdownBean = null;

    private final JmxObjectInfo jmxObjectInfo;
    private final ConfigurationService config;
    private GCMonitor gcMonitor;

    @Inject
    public Main(ConfigurationService config) {
        this.config = config;
        this.jmxObjectInfo = new JmxObjectInfo(
                "SQLLAYER",
                new ManageMXBeanImpl(),
                ManageMXBean.class
        );
    }

    @Override
    public void start() {
        int interval = Integer.parseInt(config.getProperty(GC_INTERVAL_NAME));
        int logThreshold = Integer.parseInt(config.getProperty(GC_THRESHOLD_NAME));
        if(interval > 0) {
            gcMonitor = new GCMonitor(interval, logThreshold);
            gcMonitor.start();
        }
    }

    @Override
    public void stop() 
    {
        if(gcMonitor != null) {
            gcMonitor.stopRunning();
            gcMonitor = null;
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
        return config.getProperty(NAME_PROP);
    }

    @Override
    public LayerVersionInfo getVersionInfo() {
        return VERSION_INFO;
    }

    public interface ShutdownMXBean {
        public void shutdown();
    }

    private static class ShutdownMXBeanImpl implements ShutdownMXBean {
        private static final String BEAN_NAME = "com.foundationdb:type=SHUTDOWN";
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
                LOG.error("Problem stopping services", e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if(IS_STD_TO_LOG) {
            System.setErr(new PrintStream(LoggingStream.forError(LOG), true));
            System.setOut(new PrintStream(LoggingStream.forInfo(LOG), true));
        }

        try {
            doStartup();
        } catch(Throwable t) {
            LOG.error("Problem starting system", t);
            System.exit(1);
        }

        // Services started successfully, write pid to file.
        try {
            writePid();
        } catch(IOException e) {
            LOG.warn("Problem writing pid file {}", PID_FILE_NAME, e);
            // Do not abort on error as init scripts handle this fine.
        }
    }

    private static void doStartup() throws Exception {
        final ServiceManager serviceManager = new GuicedServiceManager();

        Main.shutdownBean = new ShutdownMXBeanImpl(serviceManager);

        // JVM shutdown hook.
        // Register before startServices() so services are still brought down on startup error.
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                shutdownBean.shutdown();
            }
        }, "ShutdownHook"));

        // Bring system up
        serviceManager.startServices();

        ObjectName name = new ObjectName(ShutdownMXBeanImpl.BEAN_NAME);
        ManagementFactory.getPlatformMBeanServer().registerMBean(shutdownBean, name);
    }

    private static void writePid() throws IOException {
        if (PID_FILE_NAME != null) {
            File pidFile = new File(PID_FILE_NAME);
            pidFile.deleteOnExit();
            FileWriter out = new FileWriter(pidFile);
            out.write(OsUtils.getProcessID());
            out.flush();
        }
    }


    /**
     * Start from procrun.
     * @see <a href="http://commons.apache.org/daemon/procrun.html">Daemon: Procrun</a>
     */
    private static final Semaphore PROCRUN_SEMAPHORE = new Semaphore(0);

    private static boolean isProcrunJVMMode(String[] args) {
        if(args.length != 1) {
            String message = "Expected exactly one argument (StartMode)";
            LOG.error("{}: {}", message, Arrays.toString(args));
            throw new IllegalArgumentException(message);
        }
        return "jvm".equals(args[0]);
    }

    @SuppressWarnings("unused") // Called by procrun
    public static void procrunStart(String[] args) throws Exception {
        boolean jvmMode = isProcrunJVMMode(args);
        main(args);
        if(jvmMode) {
            PROCRUN_SEMAPHORE.acquire();
        }
    }

    @SuppressWarnings("unused") // Called by procrun
    public static void procrunStop(String[] args) throws Exception {
        boolean jvmMode = isProcrunJVMMode(args);
        // Stop server from another thread.
        shutdownBean.shutdown();
        if(jvmMode) {
            PROCRUN_SEMAPHORE.release();
        }
    }
}
