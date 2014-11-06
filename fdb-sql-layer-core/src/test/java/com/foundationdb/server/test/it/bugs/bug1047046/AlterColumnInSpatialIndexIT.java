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

package com.foundationdb.server.test.it.bugs.bug1047046;

import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.server.test.it.dxl.AlterTableITBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AlterColumnInSpatialIndexIT extends AlterTableITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t1";
    private static final String INDEX_NAME = "idx1";
    private static final int ROW_COUNT = 3;

    public void createAndLoadTable() {
        int tid = createTable(SCHEMA, TABLE, "c1 decimal(11,7), c2 decimal(11,7)");
        writeRows(
                row(tid, "43.5435", "156.989"),
                row(tid, "32.456", "99.543"),
                row(tid, "53.00", "80.00")
        );
        createIndex(SCHEMA, TABLE, INDEX_NAME, "z_order_lat_lon(c1, c2)");
        TableIndex index = getTable(tid).getIndex("idx1");
        assertNotNull("Found index", index);
        assertEquals("Is spatial", true, index.isSpatial());
    }

    // From bug report
    @Test
    public void alterToIncompatible() {
        createAndLoadTable();
        runAlter("ALTER TABLE t1 ALTER c2 SET DATA TYPE varchar(10)");
        final int tid = tableId(SCHEMA, TABLE);
        assertEquals("row count", ROW_COUNT, scanAll(tid).size());
        assertEquals("Index exists", false, getTable(tid).getIndex(INDEX_NAME) != null);
    }

    @Test
    public void alterToCompatible() {
        createAndLoadTable();
        final int tid = tableId(SCHEMA, TABLE);
        TableIndex indexBefore = getTable(tid).getIndex(INDEX_NAME);
        assertEquals("index row count before alter", ROW_COUNT, scanAllIndex(indexBefore).size());
        runAlter("ALTER TABLE t1 ALTER c2 SET DATA TYPE decimal(22,14)");
        assertEquals("row count", ROW_COUNT, scanAll(tid).size());
        TableIndex index = getTable(tid).getIndex(INDEX_NAME);
        assertEquals("Index exists", true, index != null);
        assertEquals("index row count", ROW_COUNT, scanAllIndex(index).size());
    }

    @Test
    public void dropColumn2() {
        createAndLoadTable();
        runAlter("ALTER TABLE t1 DROP COLUMN c2");
        final int tid = tableId(SCHEMA, TABLE);
        assertEquals("row count", ROW_COUNT, scanAll(tid).size());
        assertEquals("Index exists", false, getTable(tid).getIndex(INDEX_NAME) != null);
    }
    
    @Test
    public void dropColumn1() {
        createAndLoadTable();
        runAlter("ALTER TABLE t1 DROP COLUMN c1");
        final int tid = tableId(SCHEMA, TABLE);
        assertEquals("row count", ROW_COUNT, scanAll(tid).size());
        assertEquals("Index exists", false, getTable(tid).getIndex(INDEX_NAME) != null);
    }
}
