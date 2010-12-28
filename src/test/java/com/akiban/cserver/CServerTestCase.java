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
import org.junit.Ignore;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.UnitTestServiceFactory;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.cserver.service.tree.TreeService;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.PersistitStoreSchemaManager;
import com.akiban.cserver.store.SchemaManager;
import com.akiban.cserver.store.Store;
import com.persistit.Persistit;

@Ignore
public class CServerTestCase extends TestCase {

    protected Store store;
    protected SchemaManager schemaManager;
    protected ServiceManager serviceManager;
    protected RowDefCache rowDefCache;
    protected final static Session session = new SessionImpl();

    @Before
    @Override
    public void setUp() throws Exception {
        serviceManager = UnitTestServiceFactory.createServiceManager();
        serviceManager.startServices();
        store = serviceManager.getStore();
        schemaManager = serviceManager.getSchemaManager();
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

    protected SchemaManager getSchemaManager() {
        return schemaManager;
    }

    protected PersistitStore getPersistitStore() {
        return (PersistitStore) store;
    }

    protected TreeService getPersistitService() {
        return serviceManager.getPersistitService();
    }

    protected AkibaInformationSchema setUpAisForTests(final String resourceName)
            throws Exception {
        final AkibaInformationSchema ais = ((PersistitStoreSchemaManager)schemaManager).getAisForTests(resourceName);
        rowDefCache.setAIS(ais);
        CServerTestSuite.markTableStatusClean(rowDefCache);
        rowDefCache.fixUpOrdinals(schemaManager);
        return ais;
    }
}
