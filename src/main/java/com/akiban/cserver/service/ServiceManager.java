package com.akiban.cserver.service;

import java.util.HashMap;
import java.util.Map;

import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.config.ConfigurationServiceImpl;
import com.akiban.cserver.service.network.NetworkService;
import com.akiban.cserver.service.network.NetworkServiceImpl;

public class ServiceManager
{
    private static final ServiceManager instance = new ServiceManager();
    private Map<String, Service> services;
    private static final String NETWORK = "network";
    private static final String CONFIGURATION = "configuration";
    
    public static ServiceManager get()
    {
        return instance;
    }

    private ServiceManager()
    {
        services = new HashMap<String, Service>();
    }

    public void startServices()
    {
        Service config = new ConfigurationServiceImpl();
        config.start();
        services.put(CONFIGURATION, config);
        
        Service network = new NetworkServiceImpl();
        network.start();
        services.put(NETWORK, network);
    }
    
    public void stopServices()
    {
        services.get(NETWORK).stop();
        services.get(CONFIGURATION).stop();
    }
    
    public ConfigurationService getConfigurationService()
    {
        return (ConfigurationService)services.get(CONFIGURATION);
    }
    
    public NetworkService getNetworkService()
    {
        return (NetworkService)services.get(NETWORK);
    }
}
