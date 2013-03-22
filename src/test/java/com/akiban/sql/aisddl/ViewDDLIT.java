
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
