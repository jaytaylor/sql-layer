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
import static org.junit.Assert.assertTrue;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.View;
import com.akiban.server.error.UndefinedViewException;
import com.akiban.server.error.ViewReferencesExist;

import java.util.Collection;

public class ViewDDLIT extends AISDDLITBase {

    @Before
    public void createTable() throws Exception {
        executeDDL("CREATE TABLE t(id INT PRIMARY KEY NOT NULL, s VARCHAR(10))");
    }

    @Test
    public void testCreate() throws Exception {
        executeDDL("CREATE VIEW v AS SELECT * FROM t");
        View v = ais().getView(SCHEMA_NAME, "v");
        assertNotNull(v);
        assertEquals(2, v.getColumns().size());
        assertEquals("id", v.getColumn(0).getName());
        assertEquals("s", v.getColumn(1).getName());
        Table t = ais().getTable(SCHEMA_NAME, "t");
        assertEquals(1, v.getTableReferences().size());
        Collection<Column> tcols = v.getTableColumnReferences(t);
        assertNotNull(tcols);
        assertEquals(2, tcols.size());
    }

    @Test(expected=UndefinedViewException.class)
    public void testDropNonexistent() throws Exception {
        executeDDL("DROP VIEW no_such_view");
    }

    @Test
    public void testDropOptionalNonexistent() throws Exception {
        executeDDL("DROP VIEW IF EXISTS no_such_view");
    }

    @Test
    public void testDropExists() throws Exception {
        executeDDL("CREATE VIEW v AS SELECT * FROM t");
        assertNotNull(ais().getView(SCHEMA_NAME, "v"));

        executeDDL("DROP VIEW v");
        assertNull(ais().getView(SCHEMA_NAME, "v"));
    }

    @Test
    public void testDropOptionalExists() throws Exception {
        executeDDL("CREATE VIEW v AS SELECT * FROM t");
        assertNotNull(ais().getView(SCHEMA_NAME, "v"));

        executeDDL("DROP VIEW IF EXISTS v");
        assertNull(ais().getView(SCHEMA_NAME, "v"));
    }

    @Test(expected=ViewReferencesExist.class)
    public void testDropTableReferenced() throws Exception {
        executeDDL("CREATE VIEW v AS SELECT * FROM t");
        executeDDL("DROP TABLE t");
    }

    @Test(expected=ViewReferencesExist.class)
    public void testDropViewReferenced() throws Exception {
        executeDDL("CREATE VIEW v1 AS SELECT * FROM t");
        executeDDL("CREATE VIEW v2 AS SELECT * FROM v1");
        executeDDL("DROP VIEW v1");
    }

}
