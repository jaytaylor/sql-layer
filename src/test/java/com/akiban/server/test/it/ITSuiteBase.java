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

package com.akiban.server.test.it;

import com.akiban.server.AkServer;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.TestSessionFactory;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.util.MySqlStatementSplitter;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * This class differs from ITBase: this base
 * class is intended for tests that start the services, load
 * a fairly large amount of data, and then perform numerous
 * tests on the same environment. ITBase is
 * intended for tests that start and stop all the
 * services once for each test.
 */
public abstract class ITSuiteBase {

    protected static Store store;
    protected static SchemaManager schemaManager;
    protected static ServiceManager serviceManager;
    protected static RowDefCache rowDefCache;
    protected final static Session session = TestSessionFactory.get().createSession();

    @BeforeClass
    public static void setUpSuite() throws Exception {
        serviceManager = new GuicedServiceManager(GuicedServiceManager.testUrls());
        serviceManager.startServices();
        store = serviceManager.getStore();
        schemaManager = serviceManager.getSchemaManager();
        rowDefCache = store.getRowDefCache();
    }

    @AfterClass
    public static void tearDownSuite() throws Exception {
        serviceManager.stopServices();
        store = null;
        rowDefCache = null;
    }

    protected static void loadDDLFromResource(final String schema, final String resourceName) throws Exception {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                AkServer.class.getClassLoader().getResourceAsStream(resourceName)));

        DDLFunctions ddl = serviceManager.getDXL().ddlFunctions();
        for (String statement : new MySqlStatementSplitter(reader)) {
            if (statement.startsWith("create")) {
                throw new UnsupportedOperationException("Reimplement");
                //ddl.createTable(session, schema, statement);
            }
        }
    }
}
