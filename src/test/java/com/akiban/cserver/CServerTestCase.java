package com.akiban.cserver;

/**
 * Base class for a number of unit tests that need a 
 * PersistitService, PersistitStore, and Session.  These
 * are configured by UnitTestServiceManagerFactory
 * to use a temporary directory that will be cleaned up when
 * the test ends.
 */
import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.UnitTestServiceManagerFactory;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.PersistitStoreSchemaManager;
import com.akiban.cserver.store.Store;
import com.persistit.Persistit;

public class CServerTestCase extends TestCase {

    protected Store store;
    protected ServiceManager serviceManager;
    protected RowDefCache rowDefCache;
    protected final static Session session = new SessionImpl();

    @Before
    @Override
    public void setUp() throws Exception {
        serviceManager = UnitTestServiceManagerFactory.createServiceManager();
        serviceManager.startServices();
        store = serviceManager.getStore();
        rowDefCache = store.getRowDefCache();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        serviceManager.stopServices();
        store = null;
        rowDefCache = null;
    }

    protected Persistit getDb() {
        return serviceManager.getPersistitService().getDb();
    }
    
    protected PersistitStoreSchemaManager getSchemaManager() {
        return (PersistitStoreSchemaManager)store.getSchemaManager();
    }
    
    protected PersistitStore getPersistitStore() {
        return (PersistitStore)store;
    }
}
