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
     * Name of this akserver. Must match one of the entries in
     * /config/cluster.properties (managed by Admin).
     */
    private static final String AKSERVER_NAME = System.getProperty("akserver.name");

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
