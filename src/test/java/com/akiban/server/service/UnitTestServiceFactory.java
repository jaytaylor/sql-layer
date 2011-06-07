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

package com.akiban.server.service;

import java.io.File;
import java.util.Collection;

import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.config.Property;
import com.akiban.server.service.config.TestConfigService;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.jmx.JmxRegistryService;
import com.akiban.server.service.jmx.JmxRegistryServiceImpl;
import com.akiban.server.service.network.NetworkService;
import com.akiban.server.service.network.NetworkServiceImpl;

import javax.management.ObjectName;

/**
 * Extension of DefaultServiceFactory that creates mock services for unit tests.
 * Specifically, this class is used by tests that need to run the AkSserver and
 * PersistitStore code methods, but which do not need the JmxRegistryService and
 * NetworkService implementations to be functional.
 * 
 * @author peter
 * 
 */
public class UnitTestServiceFactory extends DefaultServiceFactory {
    private final MockJmxRegistryService jmxRegistryService = new MockJmxRegistryService();
    private final TestConfigService configService = new TestConfigService();
    private final MockNetworkService networkService = new MockNetworkService(
            configService);

    private final boolean withNetwork;

    public static ServiceManager createServiceManager() {
        ServiceManagerImpl.setServiceManager(null);
        return new ServiceManagerImpl(new UnitTestServiceFactory(false, null));
    }

    public static ServiceManager createServiceManagerWithNetworkService() {
        ServiceManagerImpl.setServiceManager(null);
        return new ServiceManagerImpl(new UnitTestServiceFactory(true, null));
    }

    public static ServiceManager createServiceManager(
            final Collection<Property> properties) {
        ServiceManagerImpl.setServiceManager(null);
        return new ServiceManagerImpl(new UnitTestServiceFactory(true,
                properties));
    }

    public static ServiceManager getServiceManager() {
        return ServiceManagerImpl.get();
    }

    private static class MockJmxRegistryService extends JmxRegistryServiceImpl {
        @Override
        public ObjectName register(JmxManageable service) {
            return null;
        }

        @Override
        public void unregister(String serviceName) {
            // ignore
        }

        @Override
        public void start() {
            // ignore
        }

        @Override
        public void stop() {
            // ignore
        }
    }

    private static class MockNetworkService implements NetworkService,
            Service<NetworkService> {

        public MockNetworkService(ConfigurationService config) {

        }

        @Override
        public String getNetworkHost() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getNetworkPort() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getTcpNoDelay() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getNumberOpenConnections() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getTotalNumberConnections() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getMinWorkerThreads() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getMaxWorkerThreads() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getShutdownWaitSeconds() {
            throw new UnsupportedOperationException();
        }

        @Override
        public NetworkService cast() {
            return this;
        }

        @Override
        public Class<NetworkService> castClass() {
            return NetworkService.class;
        }

        @Override
        public void start() throws Exception {
            // do nothing
        }

        @Override
        public void stop() throws Exception {
            // do nothing
        }
        
        @Override
        public void crash() throws Exception {
            // do nothing
        }
        
    }

    protected UnitTestServiceFactory(final boolean withNetwork,
            final Collection<Property> extraProperties) {
        this.withNetwork = withNetwork;
    }

    @Override
    public Service<JmxRegistryService> jmxRegistryService() {
        return jmxRegistryService;
    }

    @Override
    public Service<ConfigurationService> configurationService() {
        return configService;
    }

    @Override
    public Service<NetworkService> networkService() {
        if (withNetwork) {
            return new NetworkServiceImpl(configService);
        }
        return networkService;
    }
}
