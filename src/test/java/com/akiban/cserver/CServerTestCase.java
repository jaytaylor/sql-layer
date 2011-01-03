package com.akiban.cserver;

/**
 * Base class for a number of unit tests that need a 
 * PersistitService, PersistitStore, and Session.  These
 * are configured by UnitTestServiceManagerFactory
 * to use a temporary directory that will be cleaned up when
 * the test ends.
 */

import static com.akiban.cserver.service.tree.TreeService.SCHEMA_TREE_NAME;

import java.util.Collection;

import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.UnitTestServiceFactory;
import com.akiban.cserver.service.config.Property;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.cserver.service.tree.TreeServiceImpl;
import com.akiban.cserver.store.PersistitStore;
import com.akiban.cserver.store.PersistitStoreSchemaManager;
import com.akiban.cserver.store.SchemaManager;
import com.akiban.cserver.store.Store;
import com.persistit.Persistit;
import com.persistit.Volume;
import com.persistit.exception.PersistitException;

public abstract class CServerTestCase {

    protected Store store;
    protected SchemaManager schemaManager;
    protected ServiceManager serviceManager;
    protected RowDefCache rowDefCache;
    protected final static Session session = new SessionImpl();

    public void setUp() throws Exception {
        setUp(null);
    }

    public void setUp(final Collection<Property> extraProperties) throws Exception {
        serviceManager = UnitTestServiceFactory.createServiceManager(extraProperties);
        serviceManager.startServices();
        store = serviceManager.getStore();
        schemaManager = serviceManager.getSchemaManager();
        rowDefCache = store.getRowDefCache();
    }

    public void tearDown() throws Exception {
        serviceManager.stopServices();
        store = null;
        rowDefCache = null;
    }
    
    protected Persistit getDb() {
        return serviceManager.getTreeService().getDb();
    }

    protected SchemaManager getSchemaManager() {
        return schemaManager;
    }

    protected PersistitStore getPersistitStore() {
        return (PersistitStore) store;
    }

    protected TreeServiceImpl getTreeService() {
        return (TreeServiceImpl) serviceManager.getTreeService();
    }

    protected Volume getDefaultVolume() throws PersistitException {
        return getTreeService().mappedVolume("default", SCHEMA_TREE_NAME);
    }

    protected AkibaInformationSchema setUpAisForTests(final String resourceName)
            throws Exception {
        final AkibaInformationSchema ais = ((PersistitStoreSchemaManager) schemaManager)
                .getAisForTests(resourceName);
        rowDefCache.setAIS(ais);
        CServerTestSuite.markTableStatusClean(rowDefCache);
        rowDefCache.fixUpOrdinals(schemaManager);
        return ais;
    }
    
    protected Property property(final String module, final String name, final String value) {
        return new Property(new Property.Key(module, name), value);
    }
}
