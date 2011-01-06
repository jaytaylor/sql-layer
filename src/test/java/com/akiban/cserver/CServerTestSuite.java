package com.akiban.cserver;

/**
 * Base class for a number of unit tests that need a 
 * PersistitService, PersistitStore, and Session.  These
 * are configured by UnitTestServiceManagerFactory
 * to use a temporary directory that will be cleaned up when
 * the test ends.
 * 
 * 
 * This class differs from CServerTestCase: this base
 * class is intended for tests that start the services, load
 * a fairly large amount of data, and then perform numerous
 * tests on the same environment. CServerTestCase is 
 * intended for tests that start and stop all the
 * services once for each test.  
 */
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.UnitTestServiceFactory;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.cserver.store.PersistitStoreSchemaManager;
import com.akiban.cserver.store.SchemaManager;
import com.akiban.cserver.store.Store;

public abstract class CServerTestSuite {

    protected static Store store;
    protected static SchemaManager schemaManager;
    protected static ServiceManager serviceManager;
    protected static RowDefCache rowDefCache;
    protected final static Session session = new SessionImpl();

    public final static void setUpSuite() throws Exception {
        serviceManager = UnitTestServiceFactory.createServiceManager();
        serviceManager.startServices();
        store = serviceManager.getStore();
        schemaManager = serviceManager.getSchemaManager();
        rowDefCache = store.getRowDefCache();
    }

    public final static void tearDownSuite() throws Exception {
        serviceManager.stopServices();
        store = null;
        rowDefCache = null;
    }

    protected static AkibaInformationSchema setUpAisForTests(
            final String resourceName) throws Exception {
        final AkibaInformationSchema ais = ((PersistitStoreSchemaManager) schemaManager)
                .getAisForTests(resourceName);
        rowDefCache.clear();
        rowDefCache.setAIS(ais);
        markTableStatusClean(rowDefCache);
        rowDefCache.fixUpOrdinals(schemaManager);
        return ais;
    }

    /**
     * This call marks all TableStatus records as flushed (not dirty) without
     * actually trying to load save status records. This is only for unit tests
     * that start with empty storage.
     * 
     * @param rowDefCache
     */
    static void markTableStatusClean(final RowDefCache rowDefCache) {
        for (final RowDef rowDef : rowDefCache.getRowDefs()) {
            rowDef.getTableStatus().flushed();
        }
    }
}
