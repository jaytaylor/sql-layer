package com.akiban.cserver.service;

import com.akiban.cserver.CServer;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.config.ConfigurationServiceImpl;
import com.akiban.cserver.service.jmx.JmxRegistryService;
import com.akiban.cserver.service.jmx.JmxRegistryServiceImpl;
import com.akiban.cserver.service.logging.LoggingService;
import com.akiban.cserver.service.logging.LoggingServiceImpl;
import com.akiban.cserver.service.memcache.MemcacheService;
import com.akiban.cserver.service.memcache.MemcacheServiceImpl;
import com.akiban.cserver.service.network.NetworkService;
import com.akiban.cserver.service.network.NetworkServiceImpl;
import com.akiban.cserver.service.session.SessionService;
import com.akiban.cserver.service.session.SessionServiceImpl;
import com.akiban.cserver.service.tree.TreeService;
import com.akiban.cserver.service.tree.TreeServiceImpl;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.PersistitStoreSchemaManager;
import com.akiban.cserver.store.SchemaManager;
import com.akiban.cserver.store.Store;

public class DefaultServiceFactory implements ServiceFactory {

    private Service<JmxRegistryService> jmxRegistryService;
    private Service<LoggingService> loggingService;
    private Service<SessionService> sessionService;
    private Service<ConfigurationService> configurationService;
    private Service<NetworkService> networkService;
    private Service<CServer> chunkserverService;

    private Service<TreeService> treeService;
    private Service<Store> storeService;
    private Service<SchemaManager> schemaService;
    private Service<MemcacheService> memcacheService;
    
    @Override
    public Service<ConfigurationService> configurationService() {
        if (configurationService == null) {
            configurationService = new ConfigurationServiceImpl();
        }
        return configurationService;
    }

    @Override
    public Service<LoggingService> loggingService() {
        if (loggingService == null) {
            loggingService = new LoggingServiceImpl();
        }
        return loggingService;
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
    public Service<CServer> chunkserverService() {
        if (chunkserverService == null) {
            final CServer chunkserver = new CServer();
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
    
    public Service<MemcacheService> memcacheService()
    {
        if (memcacheService == null)
        {
            memcacheService = new MemcacheServiceImpl();
        }
        return memcacheService;
    }
}
