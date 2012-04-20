/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.it;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.server.AkServer;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.rowdata.SchemaFactory;
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
import java.util.ArrayList;
import java.util.List;

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

        List<String> allStatements = new ArrayList<String>();

        DDLFunctions ddl = serviceManager.getDXL().ddlFunctions();
        for (String statement : new MySqlStatementSplitter(reader)) {
            if (statement.startsWith("create")) {
                allStatements.add(statement);
            }
        }

        SchemaFactory schemaFactory = new SchemaFactory(schema);
        AkibanInformationSchema tempAIS = schemaFactory.ais(allStatements.toArray(new String[allStatements.size()]));
        for(UserTable table : tempAIS.getUserTables().values()) {
            ddl.createTable(session, table);
        }
    }
}
