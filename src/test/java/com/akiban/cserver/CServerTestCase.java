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

package com.akiban.cserver;

/**
 * Base class for a number of unit tests that need a 
 * PersistitService, PersistitStore, and Session.  These
 * are configured by UnitTestServiceManagerFactory
 * to use a temporary directory that will be cleaned up when
 * the test ends.
 * 
 * This class differs from CServerTestSuite: this base
 * class is intended for tests that start and stop all the
 * services once for each test.  CServerTestSuite
 * is intended for tests that start the services, load
 * a fairly large amount of data, and then perform numerous
 * tests on the same environment.
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
import com.persistit.Volume;
import com.persistit.exception.PersistitException;

public abstract class CServerTestCase {

    protected Store store;
    protected SchemaManager schemaManager;
    protected ServiceManager serviceManager;
    protected RowDefCache rowDefCache;
    protected Session session = new SessionImpl();

    public final void baseSetUp() throws Exception {
        baseSetUp(null);
    }

    public final void baseSetUp(final Collection<Property> extraProperties) throws Exception {
        serviceManager = UnitTestServiceFactory.createServiceManager(extraProperties);
        serviceManager.startServices();
        store = serviceManager.getStore();
        schemaManager = serviceManager.getSchemaManager();
        rowDefCache = store.getRowDefCache();
        session = new SessionImpl();
    }

    public final void baseTearDown() throws Exception {
        serviceManager.stopServices();
        store = null;
        rowDefCache = null;
        session = null;
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
        rowDefCache.clear();
        rowDefCache.setAIS(ais);
        CServerTestSuite.markTableStatusClean(rowDefCache);
        rowDefCache.fixUpOrdinals(schemaManager);
        return ais;
    }
    
    protected Property property(final String module, final String name, final String value) {
        return new Property(new Property.Key(module, name), value);
    }
}
