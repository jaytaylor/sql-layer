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

import com.akiban.server.service.config.ModuleConfiguration;
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

import java.io.IOException;

/**
 * @author peter
 */
public class AkServer implements Service<AkServer>, JmxManageable {

    private static final String VERSION_STRING_FILE = "version/akserver_version";
    public static final String VERSION_STRING = getVersionString();

    private static final Logger LOG = LoggerFactory.getLogger(AkServer.class.getName());

    /**
     * Config property name and default for the port on which the AkSserver will
     * listen for requests.
     *
     * /** Port on which the AkSserver will listen for requests.
     */
    private final int AKSERVER_PORT;

    /**
     * Interface on which this akserver instance will listen. TODO - allow
     * multiple NICs
     */

    private final String AKSERVER_HOST;

    private static final boolean TCP_NO_DELAY =
        Boolean.parseBoolean(System.getProperty("com.akiban.server.tcpNoDelay", "true"));

    /**
     * Name of this chunkserver. Must match one of the entries in
     * /config/cluster.properties (managed by Admin).
     */
    private static final String AKSERVER_NAME = System.getProperty("akserver.name");

    private final JmxObjectInfo jmxObjectInfo;

    public AkServer() {
        this.jmxObjectInfo = new JmxObjectInfo("AKSERVER", new ManageMXBeanImpl(
                this), ManageMXBean.class);
        ModuleConfiguration config
                = ServiceManagerImpl.get().getConfigurationService().getModuleConfiguration("akserver");
        AKSERVER_PORT = Integer.parseInt( config.getProperty("port") );
        AKSERVER_HOST = config.getProperty("host");
    }

    @Override
    public void start() throws Exception {
        LOG.warn(String.format("Starting chunkserver %s on port %s",
                AKSERVER_NAME, AKSERVER_PORT));
        Tap.registerMXBean();
        LOG.warn(String.format("Started chunkserver %s on port %s", AKSERVER_NAME, AKSERVER_PORT));
    }

    @Override
    public void stop() throws Exception
    {
        Tap.unregisterMXBean();
    }

    public String host() {
        return AKSERVER_HOST;
    }

    public int port() {
        return AKSERVER_PORT;
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
    public Class<AkServer> castClass() {
        return AkServer.class;
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

    /**
     * @param args
     *            the command line arguments
     */
    public static void main(String[] args) throws Exception
    {
        final ServiceManager serviceManager = new ServiceManagerImpl(new DefaultServiceFactory());
        serviceManager.startServices();
    }
}
