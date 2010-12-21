package com.akiban.cserver.service;

import com.akiban.cserver.CServer;
import com.akiban.cserver.service.logging.LoggingService;
import com.akiban.cserver.service.persistit.PersistitService;
import com.akiban.cserver.service.session.SessionService;
import com.akiban.cserver.store.SchemaManager2;
import com.akiban.cserver.store.Store;

public interface ServiceManager extends ServiceManagerMXBean {

    void startServices() throws Exception;

    void stopServices() throws Exception;

    CServer getCServer();

    Store getStore();

    LoggingService getLogging();
    
    SessionService getSessionService();
    
    PersistitService getPersistitService();
    
    SchemaManager2 getSchemaManager();
}
