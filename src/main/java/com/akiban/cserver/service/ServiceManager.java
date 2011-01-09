package com.akiban.cserver.service;

import com.akiban.cserver.CServer;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.logging.LoggingService;
import com.akiban.cserver.service.session.SessionService;
import com.akiban.cserver.service.tree.TreeService;
import com.akiban.cserver.store.SchemaManager;
import com.akiban.cserver.store.Store;

public interface ServiceManager extends ServiceManagerMXBean {

    void startServices() throws Exception;

    void stopServices() throws Exception;

    ConfigurationService getConfigurationService();
    
    CServer getCServer();

    Store getStore();

    LoggingService getLogging();
    
    SessionService getSessionService();
    
    TreeService getTreeService();
    
    SchemaManager getSchemaManager();
}
