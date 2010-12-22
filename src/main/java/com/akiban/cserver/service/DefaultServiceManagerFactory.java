package com.akiban.cserver.service;

import com.akiban.cserver.CServer;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.config.ConfigurationServiceImpl;
import com.akiban.cserver.service.logging.LoggingService;
import com.akiban.cserver.service.network.NetworkService;
import com.akiban.cserver.service.network.NetworkServiceImpl;
import com.akiban.cserver.service.memcache.MemcacheService;
import com.akiban.cserver.store.Store;

public class DefaultServiceManagerFactory implements ServiceManagerFactory
{
    private ServiceManager serviceManager;
    private Service configurationService;
    private Service networkService;
    private Service chunkserverService;
    private Service memcacheService;
    
    @Override
    public ServiceManager serviceManager()
    {
        if (serviceManager == null) {
            serviceManager = new ServiceManagerImpl(this);
        }
        return serviceManager;
    }

    @Override
    public Service<ConfigurationService> configurationService()
    {
        if (configurationService == null) {
            configurationService = new ConfigurationServiceImpl();
        }
        return configurationService;
    }

    @Override
    public Service<LoggingService> loggingService()
    {
        assert false : "Not implemented yet";
        return null;
    }

    @Override
    public Service<NetworkService> networkService()
    {
        if (networkService == null) {
            ConfigurationService config = configurationService().cast();
            networkService = new NetworkServiceImpl(config);
        }
        return networkService;
    }

    @Override
    public Service<Store> storeService()
    {
        // Store is still allocated in ServiceManagerImpl. Need to clean up CServerConfig.
        assert false : "not implemented yet";
        return null;
    }

    @Override
    public Service<CServer> chunkserverService()
    {
        if (chunkserverService == null)
        {
            final CServer chunkserver = new CServer(serviceManager);
            chunkserverService = chunkserver;
        }
        return chunkserverService;
    }

    @Override
    public Service memcacheService()
    {
        if (memcacheService == null)
        {
            memcacheService = new MemcacheService(serviceManager);
        }
        return memcacheService;
    }
}
