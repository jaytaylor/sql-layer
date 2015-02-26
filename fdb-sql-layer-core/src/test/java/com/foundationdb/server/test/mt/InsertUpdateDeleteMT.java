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

package com.foundationdb.server.test.mt;

import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.mt.util.MonitoredThread;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import com.foundationdb.server.test.mt.util.ThreadMonitor.Stage;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilderImpl;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.server.test.mt.util.TimeMarkerComparison;
import com.foundationdb.sql.parser.IsolationLevel;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

/** Basic isolation between SELECTs and concurrent DML. */
public final class InsertUpdateDeleteMT extends MTBase
{
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";

    private int tID;
    private int pkID;
    private int xID;
    TableRowType tableRowType;
    List<Row> groupRows;
    List<Row> pkRows;
    List<Row> indexRows;

    @Before
    public void createAndLoad() {
        Assume.assumeThat(
            txnService().actualIsolationLevel(IsolationLevel.SNAPSHOT_ISOLATION_LEVEL),
            is(equalTo(IsolationLevel.SNAPSHOT_ISOLATION_LEVEL))
        );
        tID = createTable(SCHEMA, TABLE, "id INT NOT NULL PRIMARY KEY, x INT, y INT");
        createIndex(SCHEMA, TABLE, "x", "x");
        tableRowType = SchemaCache.globalSchema(ais()).tableRowType(tID);
        pkID = tableRowType.table().getPrimaryKey().getIndex().getIndexId();
        xID = tableRowType.table().getIndex("x").getIndexId();
        writeRows(row(tID, 3, 30, 300),
                  row(tID, 4, 40, 400),
                  row(tID, 6, 60, 600),
                  row(tID, 7, 70, 700));
        groupRows = runPlanTxn(groupScanCreator(tID));
        pkRows = runPlanTxn(indexScanCreator(tID, pkID));
        indexRows = runPlanTxn(indexScanCreator(tID, xID));
    }

    //
    // Insert
    //

    @Test
    public void groupScanAndInsert() {
        Row row = testRow(tableRowType, 5, 50, 500);
        test(groupScanCreator(tID), insertCreator(tID, row), groupRows, insert(groupRows, 2, row));
    }

    @Test
    public void pkScanAndInsert() {
        Row row = testRow(tableRowType, 5, 50, 500);
        Row pkRow = testRow(tableRowType.indexRowType(pkID), 5);
        test(indexScanCreator(tID, pkID), insertCreator(tID, row), pkRows, insert(pkRows, 2, pkRow));
    }

    @Test
    public void indexScanAndInsert() {
        Row row = testRow(tableRowType, 5, 50, 500);
        Row xRow = testRow(tableRowType.indexRowType(xID), 50, 5);
        test(indexScanCreator(tID, xID), insertCreator(tID, row), indexRows, insert(indexRows, 2, xRow));
    }

    //
    // Update
    //

    @Test
    public void groupScanAndUpdate() {
        Row oldRow = groupRows.get(0);
        Row newRow = testRow(tableRowType, 3, 30, 301); // Does not affect position
        test(groupScanCreator(tID), updateCreator(tID, oldRow, newRow), groupRows, replace(groupRows, 0, newRow));
    }

    @Test
    public void pkScanAndUpdate() {
        Row oldRow = groupRows.get(0);
        Row newRow = testRow(tableRowType, 1, 30, 300); // Moves row prior to first
        Row newPKRow = testRow(tableRowType.indexRowType(pkID), 1);
        test(indexScanCreator(tID, pkID), updateCreator(tID, oldRow, newRow), pkRows, replace(pkRows, 0, newPKRow));
    }

    @Test
    public void indexScanAndUpdate() {
        Row oldRow = groupRows.get(0);
        Row newRow = testRow(tableRowType, 3, 300, 300); // Moves row after last
        Row newXRow = testRow(tableRowType.indexRowType(xID), 300, 3);
        test(indexScanCreator(tID, xID), updateCreator(tID, oldRow, newRow), indexRows, combine(remove(indexRows, 0), newXRow));
    }

    //
    // Delete
    //

    @Test
    public void groupScanAndDelete() {
        Row row = groupRows.get(0);
        test(groupScanCreator(tID), deleteCreator(tID, row), groupRows, groupRows.subList(1, groupRows.size()));
    }

    @Test
    public void pkScanAndDelete() {
        Row row = groupRows.get(0);
        test(indexScanCreator(tID, pkID), deleteCreator(tID, row), pkRows, pkRows.subList(1, pkRows.size()));
    }

    @Test
    public void indexScanAndDelete() {
        Row row = groupRows.get(0);
        test(indexScanCreator(tID, xID), deleteCreator(tID, row), indexRows, indexRows.subList(1, indexRows.size()));
    }

    //
    // Internal
    //

    private void test(OperatorCreator scan, OperatorCreator dml, List<Row> startingRows, List<Row> finalRows) {
        List<MonitoredThread> threads = readAndDML(scan, dml);
        assertEquals("dml scan size", 1, threads.get(1).getScannedRows().size());
        compareRows(startingRows, threads.get(0).getScannedRows());
        compareRows(finalRows, runPlanTxn(scan));
    }

    /**  Plan that ensures reader starts transaction first but doesn't perform any reads until write is finished. */
    private List<MonitoredThread> readAndDML(OperatorCreator readPlan, OperatorCreator dmlPlan) {
        List<MonitoredThread> threads = ConcurrentTestBuilderImpl
            .create()
            .add("Scan", readPlan)
            .sync("a", Stage.POST_BEGIN)
            .sync("b", Stage.PRE_SCAN)
            .mark(Stage.PRE_BEGIN, Stage.PRE_SCAN)
            .add("DML", dmlPlan)
            .sync("a", Stage.PRE_BEGIN)
            .sync("b", Stage.FINISH)
            .mark(Stage.PRE_BEGIN, Stage.POST_COMMIT)
            .build(this);
        ThreadHelper.runAndCheck(threads);
        new TimeMarkerComparison(threads).verify("Scan:PRE_BEGIN",
                                                 "DML:PRE_BEGIN",
                                                 "DML:POST_COMMIT",
                                                 "Scan:PRE_SCAN");
        return threads;
    }
}
