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

package com.foundationdb.server.test.it.dxl;

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public final class TruncateTableIT extends ITBase {
    @Test
    public void basic() throws InvalidOperationException {
        int tableId = createTable("test", "t", "id int not null primary key");

        final int rowCount = 5;
        for(int i = 1; i <= rowCount; ++i) {
            writeRow(tableId, i);
        }
        expectRowCount(tableId, rowCount);

        dml().truncateTable(session(), tableId);

        // Check table stats
        expectRowCount(tableId, 0);

        // Check table scan
        List<Row> rows = scanAll(tableId);
        assertEquals("Rows scanned", 0, rows.size());
    }

    /*
     * Bug appeared after truncate was implemented as 'scan all rows and delete'.
     * Manifested as a corrupt RowData when doing a non-primary index scan after the truncate.
     * Store requires all index columns be present in the passed row.
     */
    @Test
    public void bug687225() throws InvalidOperationException {
        int tableId = createTable("test",
                                  "t",
                                  "id int NOT NULL, pid int NOT NULL, PRIMARY KEY(id), UNIQUE(pid)");

        writeRows(row(tableId, 1, 1),
                  row(tableId, 2, 2));
        expectRowCount(tableId, 2);

        dml().truncateTable(session(), tableId);

        int indexId = ddl().getAIS(session()).getTable("test", "t").getIndex("pid").getIndexId();

        // Exception originally thrown during dml.doScan: Corrupt RowData at {1,(long)1}
        List<Row> rows = scanAll(tableId, indexId);
        assertEquals("Rows scanned", 0, rows.size());
    }

    @Test
    public void multipleIndex() throws InvalidOperationException {
        int tableId = createTable("test",
                                  "t",
                                  "id int not null primary key, tag int, value decimal(10,2), other char(1), name varchar(32), unique(name)");
        createIndex("test", "t", "value", "value");

        writeRows(row(tableId, 1, 1234, "10.50", "a", "foo"),
                  row(tableId, 2, -421, "14.99", "b", "bar"),
                  row(tableId, 3, 1337, "100.5", "c", "zap"),
                  row(tableId, 4, -987, "12.95", "d", "dob"),
                  row(tableId, 5, 3409, "99.00", "e", "eek"));
        expectRowCount(tableId, 5);

        dml().truncateTable(session(), tableId);

        // Check table stats
        expectRowCount(tableId, 0);

        final Table table = ddl().getTable(session(), tableId);
        final int pkeyIndexId = table.getIndex("PRIMARY").getIndexId();
        final int valueIndexId = table.getIndex("value").getIndexId();
        final int nameIndexId = table.getIndex("name").getIndexId();

        // Scan on primary key
        List<Row> rows = scanAll(tableId, pkeyIndexId);
        assertEquals("Rows scanned", 0, rows.size());

        // Scan on index
        rows = scanAll(tableId, valueIndexId);
        assertEquals("Rows scanned", 0, rows.size());

        // Scan on unique index
        rows = scanAll(tableId, nameIndexId);
        assertEquals("Rows scanned", 0, rows.size());

        // Table scan
        rows = scanAll(tableId);
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

        writeRow(parentId, 1);
        expectRowCount(parentId, 1);

        dml().truncateTable(session(), parentId);

        expectRowCount(parentId, 0);
        List<Row> rows = scanAll(parentId);
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

        writeRow(parentId, 1);
        expectRowCount(parentId, 1);

        writeRow(childId, 1, 1);
        expectRowCount(childId, 1);

        dml().truncateTable(session(), parentId);

        expectRowCount(parentId, 0);
        List<Row> rows = scanAll(parentId);
        assertEquals("Rows scanned", 0, rows.size());

        expectRowCount(childId, 1);
        rows = scanAll(childId);
        assertEquals("Rows scanned", 1, rows.size());
    }

    @Test
    public void tableWithNoPK() throws InvalidOperationException {
        int tableId = createTable("test", "t", "c1 CHAR(10) NOT NULL");

        writeRows(row(tableId, "a"),
                  row(tableId, "aaa"),
                  row(tableId, "b"),
                  row(tableId, "b"),
                  row(tableId, "bb"),
                  row(tableId, "bbb"),
                  row(tableId, "c"));
        expectRowCount(tableId, 7);

        dml().truncateTable(session(), tableId);

        // Check table stats
        expectRowCount(tableId, 0);

        // Check table scan
        List<Row> rows = scanAll(tableId);
        assertEquals("Rows scanned", 0, rows.size());
    }

    // bug1024501: Truncate table incorrectly deletes entire group
    @Test
    public void truncateOFromCOIAllWithRows() throws InvalidOperationException {
        int cId = createTable("test", "c", "id int not null primary key");
        int oId = createTable("test", "o", "id int not null primary key, cid int, grouping foreign key (cid) references c(id)");
        int iId = createTable("test", "i", "id int not null primary key, oid int, grouping foreign key (oid) references o(id)");

        writeRow(cId, 1);
        writeRow(oId, 10, 1);
        writeRow(iId, 100, 10);
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

    @Test
    public void truncateCascade() throws InvalidOperationException {
        int cId = createTable("test", "c", "id int not null primary key");
        int oId = createTable("test", "o", "id int not null primary key, cid int, grouping foreign key (cid) references c(id)");
        int iId = createTable("test", "i", "id int not null primary key, oid int, grouping foreign key (oid) references o(id)");
        int aId = createTable("test", "a", "id int not null primary key, cid int, grouping foreign key (cid) references c(id)");

        writeRow(cId, 1);
        writeRow(oId, 10, 1);
        writeRow(iId, 100, 10);
        writeRow(aId, 10, 1);
        expectRowCount(cId, 1);
        expectRowCount(oId, 1);
        expectRowCount(iId, 1);
        expectRowCount(aId, 1);

        dml().truncateTable(session(), cId, true);
        expectRowCount(cId, 0);
        expectRowCount(oId, 0);
        expectRowCount(iId, 0);
        expectRowCount(aId, 0);

        writeRow(cId, 1);
        writeRow(oId, 10, 1);
        writeRow(iId, 100, 10);
        writeRow(aId, 10, 1);
        expectRowCount(cId, 1);
        expectRowCount(oId, 1);
        expectRowCount(iId, 1);
        expectRowCount(aId, 1);

        dml().truncateTable(session(), oId, true);
        expectRowCount(cId, 1);
        expectRowCount(oId, 0);
        expectRowCount(iId, 0);
        expectRowCount(aId, 1);

        dml().truncateTable(session(), cId, true);
        expectRowCount(cId, 0);
        expectRowCount(oId, 0);
        expectRowCount(iId, 0);
        expectRowCount(aId, 0);

        writeRow(oId, 10, 1);
        writeRow(iId, 100, 10);
        expectRowCount(cId, 0);
        expectRowCount(oId, 1);
        expectRowCount(iId, 1);
        expectRowCount(aId, 0);

        dml().truncateTable(session(), oId, true);
        expectRowCount(cId, 0);
        expectRowCount(oId, 0);
        expectRowCount(iId, 0);
        expectRowCount(aId, 0);
    }
}
