package com.akiban.cserver.service;

import com.akiban.cserver.CServer;
import com.akiban.cserver.CServerRequestHandler;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.config.ConfigurationServiceImpl;
import com.akiban.cserver.service.network.NetworkServiceImpl;
import com.akiban.message.ExecutionContext;

public class DefaultServiceManagerFactory implements ServiceManagerFactory
{
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
            String host = config.getProperty("cserver", "host", "0.0.0.0");
            int port = Integer.parseInt(config.getProperty("cserver", "port", "5140"));
            boolean tcpNoDelay = Boolean.parseBoolean(config.getProperty("cserver", "tcp_no_delay", "true"));
            networkService = new NetworkServiceImpl(new CServerRequestHandler(host, port, tcpNoDelay));
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
        if (chunkserverService == null) {
            final CServer chunkserver = new CServer(serviceManager);
            ((NetworkServiceImpl)networkService).executionContext(chunkserver);
            chunkserverService = chunkserver;
        }
        return chunkserverService;
    }

    // Object state

    private ServiceManager serviceManager;
    private Service configurationService;
    private Service networkService;
    private Service chunkserverService;
}
