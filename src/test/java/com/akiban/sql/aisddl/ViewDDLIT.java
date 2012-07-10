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

package com.akiban.sql.aisddl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

import java.sql.Statement;
import java.sql.SQLException;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.View;
import com.akiban.sql.pg.PostgresServerITBase;

import com.akiban.server.service.config.Property;
import com.akiban.server.service.is.BasicInfoSchemaTablesService;
import com.akiban.server.service.is.BasicInfoSchemaTablesServiceImpl;
import com.akiban.server.service.servicemanager.GuicedServiceManager;

import java.util.Collection;

public class ViewDDLIT extends PostgresServerITBase {
    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bind(BasicInfoSchemaTablesService.class, BasicInfoSchemaTablesServiceImpl.class)
;
    }

    @Override
    protected Collection<Property> startupConfigProperties() {
        return uniqueStartupConfigProperties(getClass());
    }

    private Statement stmt;

    @Before
    public void createStatement() throws Exception {
        stmt = getConnection().createStatement();
        
        stmt.executeUpdate("CREATE TABLE t(id INT PRIMARY KEY NOT NULL, s VARCHAR(10))");
    }

    @After
    public void closeStatement() throws Exception {
        stmt.close();
    }

    public void testCreate() throws Exception {
        stmt.executeUpdate("CREATE VIEW v AS SELECT * FROM t");
        View v = ddl().getAIS(session()).getView(SCHEMA_NAME, "v");
        assertNotNull(v);
        assertEquals(2, v.getColumns().size());
        assertEquals("id", v.getColumn(0).getName());
        assertEquals("s", v.getColumn(1).getName());
        Table t = ddl().getAIS(session()).getTable(SCHEMA_NAME, "t");
        assertEquals(1, v.getTableReferences().size());
        Collection<Column> tcols = v.getTableColumnReferences(t);
        assertNotNull(tcols);
        assertEquals(2, tcols.size());
    }

    @Test(expected=SQLException.class)
    public void testDropNonexistent() throws Exception {
        stmt.executeUpdate("DROP VIEW no_such_view");
    }

    @Test
    public void testDropOptionalNonexistent() throws Exception {
        stmt.executeUpdate("DROP VIEW IF EXISTS no_such_view");
    }

    @Test
    public void testDropExists() throws Exception {
        stmt.executeUpdate("CREATE VIEW v AS SELECT * FROM t");
        assertNotNull(ddl().getAIS(session()).getView(SCHEMA_NAME, "v"));

        stmt.executeUpdate("DROP VIEW v");
        assertNull(ddl().getAIS(session()).getView(SCHEMA_NAME, "v"));
    }

    @Test
    public void testDropOptionalExists() throws Exception {
        stmt.executeUpdate("CREATE VIEW v AS SELECT * FROM t");
        assertNotNull(ddl().getAIS(session()).getView(SCHEMA_NAME, "v"));

        stmt.executeUpdate("DROP VIEW IF EXISTS v");
        assertNull(ddl().getAIS(session()).getView(SCHEMA_NAME, "v"));
    }

    @Test(expected=SQLException.class)
    public void testDropTableReferenced() throws Exception {
        stmt.executeUpdate("CREATE VIEW v AS SELECT * FROM t");
        stmt.executeUpdate("DROP TABLE t");
    }

    @Test(expected=SQLException.class)
    public void testDropViewReferenced() throws Exception {
        stmt.executeUpdate("CREATE VIEW v1 AS SELECT * FROM t");
        stmt.executeUpdate("CREATE VIEW v2 AS SELECT * FROM v1");
        stmt.executeUpdate("DROP VIEW v1");
    }

    @Test
    public void testSystemView() throws Exception {
        serviceManager().getServiceByClass(BasicInfoSchemaTablesService.class);
        stmt.executeUpdate("CREATE VIEW v AS SELECT table_name FROM information_schema.tables WHERE table_schema <> 'information_schema'");
        forgetConnection();
        safeRestartTestServices();
    }

}
