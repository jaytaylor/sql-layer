package com.akiban.cserver.service;

import com.akiban.cserver.CServer;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.logging.LoggingService;
import com.akiban.cserver.service.network.NetworkService;
import com.akiban.cserver.store.Store;

public interface ServiceManagerFactory
{
    ServiceManager serviceManager();

    Service<ConfigurationService> configurationService();

    Service<LoggingService> loggingService();

    Service<NetworkService> networkService();

    Service<Store> storeService();

    Service<CServer> chunkserverService();

    Service memcacheService();
}
