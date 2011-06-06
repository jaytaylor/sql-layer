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
import com.google.inject.Guice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class GuicedServiceManager implements ServiceManager {
    // ServiceManager interface

    @Override
    public void startServices() throws ServiceStartupException {
        List<Class<?>> directlyRequiredClasses = new ArrayList<Class<?>>();
        for (ServiceBinding binding : directlyRequired) {
            Class<?> classReference = Class.forName(binding.getInterfaceName());
            guicer.get(classReference);
        }
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public void stopServices() throws Exception {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public void crashServices() throws Exception {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public ConfigurationService getConfigurationService() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public AkServer getAkSserver() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public Store getStore() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public TreeService getTreeService() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public MemcacheService getMemcacheService() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public PostgresService getPostgresService() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public SchemaManager getSchemaManager() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public JmxRegistryService getJmxRegistryService() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public StatisticsService getStatisticsService() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public SessionService getSessionService() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public <T> T getServiceByClass(Class<T> serviceClass) {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    public DXLService getDXL() {
        throw new UnsupportedOperationException(); // TODO
    }

    // GuicedServiceManager interface

    public GuicedServiceManager() {
    InputStream defaultServicesStream = GuicedServiceFactory.class.getResourceAsStream("default-services.yaml");
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
}
