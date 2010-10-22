package com.akiban.cserver.service;

import com.akiban.cserver.CServer;
import com.akiban.cserver.store.Store;

public interface ServiceManager {

    void startServices() throws Exception;

    void stopServices() throws Exception;

    CServer getCServer();

    Store getStore();
}
