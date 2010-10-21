package com.akiban.cserver.service;

import com.akiban.cserver.CServer;
import com.akiban.cserver.store.Store;

public interface ServiceManager {

    CServer getCServer();

    Store getStore();
    
}
