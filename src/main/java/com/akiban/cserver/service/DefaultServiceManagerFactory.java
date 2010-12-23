package com.akiban.cserver.service;

import com.akiban.cserver.CServer;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.config.ConfigurationServiceImpl;
import com.akiban.cserver.service.jmx.JmxRegistryService;
import com.akiban.cserver.service.jmx.JmxRegistryServiceImpl;
import com.akiban.cserver.service.logging.LoggingService;
import com.akiban.cserver.service.logging.LoggingServiceImpl;
import com.akiban.cserver.service.network.NetworkService;
import com.akiban.cserver.service.network.NetworkServiceImpl;
import com.akiban.cserver.service.persistit.PersistitService;
import com.akiban.cserver.service.persistit.PersistitServiceImpl;
import com.akiban.cserver.service.session.SessionService;
import com.akiban.cserver.service.session.SessionServiceImpl;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.PersistitStoreSchemaManager;
import com.akiban.cserver.store.SchemaManager;
import com.akiban.cserver.store.Store;

public class DefaultServiceManagerFactory implements ServiceManagerFactory {

    private Service jmxRegistryService;
    private Service loggingService;
    private Service sessionService;
    private Service configurationService;
    private Service networkService;
    private Service chunkserverService;
    private Service persistitService;
    private Service storeService;
    private Service schemaService;
    
    public static ServiceManager createServiceManager() {
        return new ServiceManagerImpl(new DefaultServiceManagerFactory());
    }

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
    public Service<PersistitService> persistitService() {
        if (persistitService == null) {
            final ConfigurationService config = configurationService().cast();
            persistitService = new PersistitServiceImpl(config);
        }
        return persistitService;
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
}
