package com.akiban.cserver.service;

import com.akiban.cserver.CServer;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.jmx.JmxRegistryService;
import com.akiban.cserver.service.logging.LoggingService;
import com.akiban.cserver.service.network.NetworkService;
import com.akiban.cserver.service.persistit.PersistitService;
import com.akiban.cserver.service.session.SessionService;
import com.akiban.cserver.store.SchemaManager;
import com.akiban.cserver.store.Store;

public interface ServiceManagerFactory
{
    Service<JmxRegistryService> jmxRegistryService();

    Service<ConfigurationService> configurationService();
    
    Service<SessionService> sessionService();

    Service<LoggingService> loggingService();

    Service<NetworkService> networkService();

    Service<CServer> chunkserverService();
    
    Service<PersistitService> persistitService();
    
    Service<SchemaManager> schemaManager();
    
    Service<Store> storeService();
}
