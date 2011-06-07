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

package com.akiban.server.service.servicemanager;

import com.akiban.server.AkServer;
import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.ServiceStartupException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.jmx.JmxRegistryService;
import com.akiban.server.service.memcache.MemcacheService;
import com.akiban.server.service.servicemanager.configuration.ServiceBinding;
import com.akiban.server.service.servicemanager.configuration.yaml.YamlConfiguration;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.stats.StatisticsService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.sql.pg.PostgresService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;

public final class GuicedServiceManager implements ServiceManager {
    // ServiceManager interface

    @Override
    public void startServices() throws ServiceStartupException {
        guicer.startRequiredServices(STANDARD_SERVICE_ACTIONS);
    }

    @Override
    public void stopServices() throws Exception {
        guicer.stopAllServices(STANDARD_SERVICE_ACTIONS);
    }

    @Override
    public void crashServices() throws Exception {
        guicer.stopAllServices(CRASH_SERVICES);
    }

    @Override
    public ConfigurationService getConfigurationService() {
        return getServiceByClass(ConfigurationService.class);
    }

    @Override
    public AkServer getAkSserver() {
        return getServiceByClass(AkServer.class);
    }

    @Override
    public Store getStore() {
        return getServiceByClass(Store.class);
    }

    @Override
    public TreeService getTreeService() {
        return getServiceByClass(TreeService.class);
    }

    @Override
    public MemcacheService getMemcacheService() {
        return getServiceByClass(MemcacheService.class);
    }

    @Override
    public PostgresService getPostgresService() {
        return getServiceByClass(PostgresService.class);
    }

    @Override
    public SchemaManager getSchemaManager() {
        return getServiceByClass(SchemaManager.class);
    }

    @Override
    public JmxRegistryService getJmxRegistryService() {
        return getServiceByClass(JmxRegistryService.class);
    }

    @Override
    public StatisticsService getStatisticsService() {
        return getServiceByClass(StatisticsService.class);
    }

    @Override
    public SessionService getSessionService() {
        return getServiceByClass(SessionService.class);
    }

    @Override
    public <T> T getServiceByClass(Class<T> serviceClass) {
        return guicer.get(serviceClass, STANDARD_SERVICE_ACTIONS);
    }

    @Override
    public DXLService getDXL() {
        return getServiceByClass(DXLService.class);
    }

    // GuicedServiceManager interface

    public GuicedServiceManager() {
    InputStream defaultServicesStream = GuicedServiceManager.class.getResourceAsStream("default-services.yaml");
        if (defaultServicesStream == null) {
            throw new RuntimeException("no resource default-services.yaml");
        }
        final Reader defaultServicesReader;
        try {
            defaultServicesReader = new InputStreamReader(defaultServicesStream, "UTF-8");
        } catch (Exception e) {
            try {
                defaultServicesStream.close();
            } catch (IOException ioe) {
                LOG.error("while closing stream error", ioe);
            }
            throw new RuntimeException("while opening default services reader", e);
        }
        final Collection<ServiceBinding> bindings;
        try {
            YamlConfiguration configuration = new YamlConfiguration();
            configuration.read("default-services.yaml", defaultServicesReader);
            bindings = configuration.serviceBindings();
        } finally {
            try {
                defaultServicesReader.close();
            } catch (IOException e) {
                throw new RuntimeException("while closing reader", e); // TODO this will override YamlConfiguration exceptions
            }
        }
        try {
            guicer = Guicer.forServices(bindings);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        directlyRequired = new ArrayList<ServiceBinding>();
        for (ServiceBinding serviceBinding : bindings) {
            if (serviceBinding.isDirectlyRequired()) {
                directlyRequired.add(serviceBinding);
            }
        }
    }

    // object state

    private final Guicer guicer;
    private final Collection<ServiceBinding> directlyRequired;

    // class state

    private static final Logger LOG = LoggerFactory.getLogger(GuicedServiceManager.class);

    private static final ServiceLifecycleActions<Service<?>> CRASH_SERVICES
            = new ServiceLifecycleActions<Service<?>>() {
        @Override
        public void onStart(Service<?> service) throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onShutdown(Service<?> service) throws Exception {
            service.crash();
        }

        @Override
        public Service<?> castIfActionable(Object object) {
            return (object instanceof Service) ? (Service<?>) object : null;
        }
    };

    static final ServiceLifecycleActions<Service<?>> STANDARD_SERVICE_ACTIONS
            = new ServiceLifecycleActions<Service<?>>()
    {
        @Override
        public void onStart(Service<?> service) throws Exception {
            service.start();
        }

        @Override
        public void onShutdown(Service<?> service) throws Exception {
            service.stop();
        }

        @Override
        public Service<?> castIfActionable(Object object) {
            return (object instanceof Service) ? (Service<?>)object : null;
        }
    };
}
