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
import com.akiban.server.service.ServiceFactory;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.jmx.JmxRegistryService;
import com.akiban.server.service.memcache.MemcacheService;
import com.akiban.server.service.network.NetworkService;
import com.akiban.server.service.servicemanager.configuration.ServiceBinding;
import com.akiban.server.service.servicemanager.configuration.yaml.YamlConfiguration;
import com.akiban.server.service.session.SessionService;
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
import java.util.Collection;

public final class GuicedServiceFactory implements ServiceFactory {

    // ServiceFactory interface

    @Override
    public Service<JmxRegistryService> jmxRegistryService() {
        return service(JmxRegistryService.class);
    }

    @Override
    public Service<ConfigurationService> configurationService() {
        return service(ConfigurationService.class);
    }

    @Override
    public Service<NetworkService> networkService() {
        return service(NetworkService.class);
    }

    @Override
    public Service<AkServer> chunkserverService() {
        return service(AkServer.class);
    }

    @Override
    public Service<TreeService> treeService() {
        return service(TreeService.class);
    }

    @Override
    public Service<SchemaManager> schemaManager() {
        return service(SchemaManager.class);
    }

    @Override
    public Service<Store> storeService() {
        return service(Store.class);
    }

    @Override
    public Service<MemcacheService> memcacheService() {
        return service(MemcacheService.class);
    }

    @Override
    public Service<PostgresService> postgresService() {
        return service(PostgresService.class);
    }

    @Override
    public Service<DXLService> dxlService() {
        return service(DXLService.class);
    }

    @Override
    public Service<SessionService> sessionService() {
        return service(SessionService.class);
    }

    // GuicedServiceFactory interface

    public GuicedServiceFactory() {
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
    }

    // private methods

    private <T> Service<T> service(Class<?> forClass) {
        Object serviceAsObject = guicer.get(forClass);
        if (! (serviceAsObject instanceof Service)) {
            throw new RuntimeException(serviceAsObject.getClass() + " is not of type Service");
        }
        @SuppressWarnings("unchecked")
        final Service<T> casted = (Service<T>) serviceAsObject;
        if (!casted.castClass().equals(forClass)) {
            throw new RuntimeException(serviceAsObject.getClass() + " is not of type " + forClass);
        }
        return casted;
    }

    // object state
    private final Guicer<?> guicer;

    // class state
    private static final Logger LOG = LoggerFactory.getLogger(GuicedServiceFactory.class);
}
