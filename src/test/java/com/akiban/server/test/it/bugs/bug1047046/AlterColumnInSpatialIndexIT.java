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

package com.akiban.server.test.it.bugs.bug1047046;

import com.akiban.ais.model.TableIndex;
import com.akiban.server.test.it.dxl.AlterTableITBase;
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
                createNewRow(tid, "143.5435", "156.989"),
                createNewRow(tid, "32.456", "99.543"),
                createNewRow(tid, "53.00", "80.00")
        );
        createIndex(SCHEMA, TABLE, INDEX_NAME, "z_order_lat_lon(c1, c2)");
        TableIndex index = getUserTable(tid).getIndex("idx1");
        assertNotNull("Found index", index);
        assertEquals("Is spatial", true, index.isSpatial());
    }

    // From bug report
    @Test
    public void alterToIncompatible() {
        createAndLoadTable();
        runAlter("ALTER TABLE t1 ALTER c2 SET DATA TYPE varchar(10)");
        final int tid = tableId(SCHEMA, TABLE);
        assertEquals("row count", ROW_COUNT, scanAll(scanAllRequest(tid)).size());
        assertEquals("Index exists", false, getUserTable(tid).getIndex(INDEX_NAME) != null);
    }

    @Test
    public void alterToCompatible() {
        createAndLoadTable();
        runAlter("ALTER TABLE t1 ALTER c2 SET DATA TYPE decimal(22,14)");
        final int tid = tableId(SCHEMA, TABLE);
        assertEquals("row count", ROW_COUNT, scanAll(scanAllRequest(tid)).size());
        TableIndex index = getUserTable(tid).getIndex(INDEX_NAME);
        assertEquals("Index exists", true, index != null);
        assertEquals("index row count", ROW_COUNT, scanAllIndex(index).size());
    }

    @Test
    public void dropColumn() {
        createAndLoadTable();
        runAlter("ALTER TABLE t1 DROP COLUMN c2");
        final int tid = tableId(SCHEMA, TABLE);
        assertEquals("row count", ROW_COUNT, scanAll(scanAllRequest(tid)).size());
        assertEquals("Index exists", false, getUserTable(tid).getIndex(INDEX_NAME) != null);
    }
}
