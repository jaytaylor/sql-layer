package com.akiban.cserver.service;

public interface ServiceManagerFactory
{
    ServiceManager serviceManager();

    Service configurationService();

    Service loggingService();

    Service networkService();

    Service storeService();

    Service chunkserverService();
}
