/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server;

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
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.UnitTestServiceFactory;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import com.akiban.server.store.PersistitStoreSchemaManager;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;

public abstract class AkServerTestSuite {

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

    protected static AkibanInformationSchema setUpAisForTests(
            final String resourceName) throws Exception {
        final AkibanInformationSchema ais = ((PersistitStoreSchemaManager) schemaManager)
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
