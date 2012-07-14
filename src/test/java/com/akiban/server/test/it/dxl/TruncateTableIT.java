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

package com.akiban.server.test.it.dxl;

import com.akiban.ais.model.Table;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.EnumSet;
import java.util.List;

import static org.junit.Assert.*;

public final class TruncateTableIT extends ITBase {
    @Test
    public void basic() throws InvalidOperationException {
        int tableId = createTable("test", "t", "id int not null primary key");

        final int rowCount = 5;
        for(int i = 1; i <= rowCount; ++i) {
            dml().writeRow(session(), createNewRow(tableId, i));
        }
        expectRowCount(tableId, rowCount);

        dml().truncateTable(session(), tableId);

        // Check table stats
        expectRowCount(tableId, 0);

        // Check table scan
        List<NewRow> rows = scanAll(new ScanAllRequest(tableId, null));
        assertEquals("Rows scanned", 0, rows.size());
    }

    /*
     * Bug appeared after truncate was implemented as 'scan all rows and delete'.
     * Manifested as a corrupt RowData when doing a non-primary index scan after the truncate.
     * Turned out that PersistitStore.deleteRow() requires all index columns be present in the passed RowData.
     */
    @Test
    public void bug687225() throws InvalidOperationException {
        int tableId = createTable("test",
                                  "t",
                                  "id int NOT NULL, pid int NOT NULL, PRIMARY KEY(id), UNIQUE(pid)");

        writeRows(createNewRow(tableId, 1, 1),
                  createNewRow(tableId, 2, 2));
        expectRowCount(tableId, 2);

        dml().truncateTable(session(), tableId);

        int indexId = ddl().getAIS(session()).getTable("test", "t").getIndex("pid").getIndexId();
        EnumSet<ScanFlag> scanFlags = EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END);

        // Exception originally thrown during dml.doScan: Corrupt RowData at {1,(long)1}
        List<NewRow> rows = scanAll(new ScanAllRequest(tableId, null, indexId, scanFlags));
        assertEquals("Rows scanned", 0, rows.size());
    }

    @Test
    public void multipleIndex() throws InvalidOperationException {
        int tableId = createTable("test",
                                  "t",
                                  "id int not null primary key, tag int, value decimal(10,2), other char(1), name varchar(32), unique(name)");
        createIndex("test", "t", "value", "value");

        writeRows(createNewRow(tableId, 1, 1234, "10.50", 'a', "foo"),
                  createNewRow(tableId, 2, -421, "14.99", 'b', "bar"),
                  createNewRow(tableId, 3, 1337, "100.5", 'c', "zap"),
                  createNewRow(tableId, 4, -987, "12.95", 'd', "dob"),
                  createNewRow(tableId, 5, 3409, "99.00", 'e', "eek"));
        expectRowCount(tableId, 5);

        dml().truncateTable(session(), tableId);

        // Check table stats
        expectRowCount(tableId, 0);

        final Table table = ddl().getTable(session(), tableId);
        final int pkeyIndexId = table.getIndex("PRIMARY").getIndexId();
        final int valueIndexId = table.getIndex("value").getIndexId();
        final int nameIndexId = table.getIndex("name").getIndexId();
        final EnumSet<ScanFlag> scanFlags = EnumSet.of(ScanFlag.START_AT_BEGINNING, ScanFlag.END_AT_END);

        // Scan on primary key
        List<NewRow> rows = scanAll(new ScanAllRequest(tableId, null, pkeyIndexId, scanFlags));
        assertEquals("Rows scanned", 0, rows.size());

        // Scan on index
        rows = scanAll(new ScanAllRequest(tableId, null, valueIndexId, scanFlags));
        assertEquals("Rows scanned", 0, rows.size());

        // Scan on unique index
        rows = scanAll(new ScanAllRequest(tableId, null, nameIndexId, scanFlags));
        assertEquals("Rows scanned", 0, rows.size());

        // Table scan
        rows = scanAll(new ScanAllRequest(tableId, null));
        assertEquals("Rows scanned", 0, rows.size());
    }

    @Test
    public void truncateParentNoChildRows() throws InvalidOperationException {
        int parentId = createTable("test",
                                   "parent",
                                   "id int not null primary key");
        int childId = createTable("test",
                                   "child",
                                   "id int not null primary key, pid int, grouping foreign key (pid) references parent(id)");

        dml().writeRow(session(), createNewRow(parentId, 1));
        expectRowCount(parentId, 1);

        dml().truncateTable(session(), parentId);

        expectRowCount(parentId, 0);
        List<NewRow> rows = scanAll(new ScanAllRequest(parentId, null));
        assertEquals("Rows scanned", 0, rows.size());
    }

    @Test
    public void truncateParentWithChildRows() throws InvalidOperationException {
        int parentId = createTable("test",
                                   "parent",
                                   "id int not null primary key");
        int childId = createTable("test",
                                   "child",
                                   "id int not null primary key, pid int, grouping foreign key (pid) references parent(id)");

        dml().writeRow(session(), createNewRow(parentId, 1));
        expectRowCount(parentId, 1);

        dml().writeRow(session(), createNewRow(childId, 1, 1));
        expectRowCount(childId, 1);

        dml().truncateTable(session(), parentId);

        expectRowCount(parentId, 0);
        List<NewRow> rows = scanAll(new ScanAllRequest(parentId, null));
        assertEquals("Rows scanned", 0, rows.size());

        expectRowCount(childId, 1);
        rows = scanAll(new ScanAllRequest(childId, null));
        assertEquals("Rows scanned", 1, rows.size());
    }

    @Test
    public void tableWithNoPK() throws InvalidOperationException {
        int tableId = createTable("test", "t", "c1 CHAR(10) NOT NULL");

        writeRows(createNewRow(tableId, "a", -1L),
                  createNewRow(tableId, "aaa", -1L),
                  createNewRow(tableId, "b", -1L),
                  createNewRow(tableId, "b", -1L),
                  createNewRow(tableId, "bb", -1L),
                  createNewRow(tableId, "bbb", -1L),
                  createNewRow(tableId, "c", -1L));
        expectRowCount(tableId, 7);

        dml().truncateTable(session(), tableId);

        // Check table stats
        expectRowCount(tableId, 0);

        // Check table scan
        List<NewRow> rows = scanAll(new ScanAllRequest(tableId, null));
        assertEquals("Rows scanned", 0, rows.size());
    }

    // bug1024501: Truncate table incorrectly deletes entire group
    @Test
    public void truncateOFromCOIAllWithRows() throws InvalidOperationException {
        int cId = createTable("test", "c", "id int not null primary key");
        int oId = createTable("test", "o", "id int not null primary key, cid int, grouping foreign key (cid) references c(id)");
        int iId = createTable("test", "i", "id int not null primary key, oid int, grouping foreign key (oid) references o(id)");

        dml().writeRow(session(), createNewRow(cId, 1));
        dml().writeRow(session(), createNewRow(oId, 10, 1));
        dml().writeRow(session(), createNewRow(iId, 100, 10));
        expectRowCount(cId, 1);
        expectRowCount(oId, 1);
        expectRowCount(iId, 1);

        dml().truncateTable(session(), cId);
        expectRowCount(cId, 0);
        expectRowCount(oId, 1);
        expectRowCount(iId, 1);

        dml().truncateTable(session(), oId);
        expectRowCount(cId, 0);
        expectRowCount(oId, 0);
        expectRowCount(iId, 1);

        dml().truncateTable(session(), iId);
        expectRowCount(cId, 0);
        expectRowCount(oId, 0);
        expectRowCount(iId, 0);
    }
}
