/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.aisddl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.View;
import com.foundationdb.server.error.UndefinedViewException;
import com.foundationdb.server.error.ViewReferencesExist;

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

    @Test
    public void testCreateWithFunctions() throws Exception {
        executeDDL("CREATE VIEW v AS SELECT current_session_id() AS r");
        View v = ais().getView(SCHEMA_NAME, "v");
        assertNotNull(v);
        assertEquals(1, v.getColumns().size());
        assertEquals("r", v.getColumn(0).getName());
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

    @Test
    public void testViewColumnNames() throws Exception {
        executeDDL("CREATE VIEW v(x,y) AS SELECT id, s FROM t");
        View v = ais().getView(SCHEMA_NAME, "v");
        assertEquals("x", v.getColumns().get(0).getName());
        assertEquals("y", v.getColumns().get(1).getName());
    }
}
