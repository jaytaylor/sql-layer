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

import com.akiban.server.AkServer;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.config.ConfigurationServiceImpl;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.dxl.DXLServiceImpl;
import com.akiban.server.service.jmx.JmxRegistryService;
import com.akiban.server.service.jmx.JmxRegistryServiceImpl;
import com.akiban.server.service.memcache.MemcacheService;
import com.akiban.server.service.memcache.MemcacheServiceImpl;
import com.akiban.server.service.network.NetworkService;
import com.akiban.server.service.network.NetworkServiceImpl;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.session.SessionServiceImpl;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.service.tree.TreeServiceImpl;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.PersistitStoreSchemaManager;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;

public class DefaultServiceFactory implements ServiceFactory {

    private Service<JmxRegistryService> jmxRegistryService;
    private Service<SessionService> sessionService;
    private Service<ConfigurationService> configurationService;
    private Service<NetworkService> networkService;
    private Service<AkServer> chunkserverService;

    private Service<TreeService> treeService;
    private Service<Store> storeService;
    private Service<SchemaManager> schemaService;
    private Service<MemcacheService> memcacheService;
    private Service<DXLService> dxlService;
    
    @Override
    public Service<ConfigurationService> configurationService() {
        if (configurationService == null) {
            configurationService = new ConfigurationServiceImpl();
        }
        return configurationService;
    }

    @Override
    public Service<NetworkService> networkService() {
        if (networkService == null) {
            ConfigurationService config = configurationService().cast();
            networkService = new NetworkServiceImpl(config);
        }
        return networkService;
    }

    @Override
    public Service<AkServer> chunkserverService() {
        if (chunkserverService == null) {
            final AkServer chunkserver = new AkServer();
            chunkserverService = chunkserver;
        }
        return chunkserverService;
    }

    @Override
    public Service<JmxRegistryService> jmxRegistryService() {
        if (jmxRegistryService == null) {
            jmxRegistryService = new JmxRegistryServiceImpl();
        }
        return jmxRegistryService;
    }

    @Override
    public Service<SessionService> sessionService() {
        if (sessionService == null) {
            sessionService = new SessionServiceImpl();
        }
        return sessionService;
    }

    @Override
    public Service<TreeService> treeService() {
        if (treeService == null) {
            treeService = new TreeServiceImpl();
        }
        return treeService;
    }

    @Override
    public Service<Store> storeService() {
        if (storeService == null) {
            storeService = new PersistitStore();
        }
        return storeService;
    }

    @Override
    public Service<SchemaManager> schemaManager() {
        if (schemaService == null) {
            schemaService = new PersistitStoreSchemaManager();
        }
        return schemaService;
    }

    @Override
    public Service<MemcacheService> memcacheService()
    {
        if (memcacheService == null)
        {
            memcacheService = new MemcacheServiceImpl();
        }
        return memcacheService;
    }

    @Override
    public Service<DXLService> dxlService() {
        if (dxlService == null) {
            dxlService = new DXLServiceImpl();
        }
        return dxlService;
    }
}
