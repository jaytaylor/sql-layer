package com.akiban.cserver.service;

import com.akiban.cserver.CServer;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.config.ConfigurationServiceImpl;
import com.akiban.cserver.service.network.NetworkServiceImpl;

public class DefaultServiceManagerFactory implements ServiceManagerFactory
{
    private ServiceManager serviceManager;
    private Service configurationService;
    private Service networkService;
    private Service chunkserverService;
    
    @Override
    public ServiceManager serviceManager()
    {
        if (serviceManager == null) {
            serviceManager = new ServiceManagerImpl(this);
        }
        return serviceManager;
    }

    @Override
    public Service configurationService()
    {
        if (configurationService == null) {
            configurationService = new ConfigurationServiceImpl();
        }
        return configurationService;
    }

    @Override
    public Service loggingService()
    {
        assert false : "Not implemented yet";
        return null;
    }

    @Override
    public Service networkService()
    {
        if (networkService == null) {
            ConfigurationService config = (ConfigurationService) configurationService();
            networkService = new NetworkServiceImpl(config);
        }
        return networkService;
    }

    @Override
    public Service storeService()
    {
        // Store is still allocated in ServiceManagerImpl. Need to clean up CServerConfig.
        assert false : "not implemented yet";
        return null;
    }

    @Override
    public Service chunkserverService()
    {
        if (chunkserverService == null)
        {
            final CServer chunkserver = new CServer(serviceManager);
            chunkserverService = chunkserver;
        }
        return chunkserverService;
    }
}
