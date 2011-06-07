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

package com.akiban.server;

import com.akiban.util.OsUtils;
import com.akiban.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.manage.ManageMXBean;
import com.akiban.server.manage.ManageMXBeanImpl;
import com.akiban.server.service.DefaultServiceFactory;
import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.util.Tap;

import javax.management.ObjectName;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;

/**
 * @author peter
 */
public class AkServer implements Service<AkServerEmptyInterface>, JmxManageable, AkServerEmptyInterface {
    private static final String VERSION_STRING_FILE = "version/akserver_version";
    public static final String VERSION_STRING = getVersionString();

    private static final Logger LOG = LoggerFactory.getLogger(AkServer.class.getName());
    private static final ShutdownMXBeanImpl shutdownBean = new ShutdownMXBeanImpl();
    private static final String AKSERVER_NAME = System.getProperty("akserver.name");
    private static final String pidFileName = System.getProperty("akserver.pidfile");

    private final JmxObjectInfo jmxObjectInfo;

    public AkServer() {
        this.jmxObjectInfo = new JmxObjectInfo("AKSERVER", new ManageMXBeanImpl(this), ManageMXBean.class);
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting AkServer {}", AKSERVER_NAME);
        Tap.registerMXBean();
    }

    @Override
    public void stop() throws Exception
    {
        LOG.info("Stopping AkServer {}", AKSERVER_NAME);
        Tap.unregisterMXBean();
    }
    
    @Override
    public void crash() throws Exception {
        stop();
    }

    public ServiceManager getServiceManager()
    {
        return ServiceManagerImpl.get();
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return jmxObjectInfo;
    }

    @Override
    public AkServer cast() {
        return this;
    }

    @Override
    public Class<AkServerEmptyInterface> castClass() {
        return AkServerEmptyInterface.class;
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

        public ShutdownMXBeanImpl() {
        }

        @Override
        public void shutdown() {
            try {
                ServiceManager sm = ServiceManagerImpl.get();
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
        // JVM shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                shutdownBean.shutdown();
            }
        }, "ShutdownHook"));

        // Bring system up
        final ServiceManager serviceManager = new ServiceManagerImpl(new DefaultServiceFactory());
        serviceManager.startServices();
        
        // JMX shutdown method
        try {
            ObjectName name = new ObjectName(ShutdownMXBeanImpl.BEAN_NAME);
            ManagementFactory.getPlatformMBeanServer().registerMBean(shutdownBean, name);
        } catch(Exception e) {
            LOG.error("Exception registering shutdown bean", e);
        }
        
        // services started successfully, now create pidfile and write pid to it
        if (pidFileName != null) {
            File pidFile = new File(pidFileName);
            pidFile.deleteOnExit();
            FileWriter out = new FileWriter(pidFile);
            out.write(OsUtils.getProcessID());
            out.flush();
        }
    }
}
