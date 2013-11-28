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

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.UpdateFunction;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import com.foundationdb.server.test.mt.util.ThreadMonitor.Stage;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilderImpl;
import com.foundationdb.server.test.mt.util.MonitoredOperatorThread;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.server.test.mt.util.TimeMarkerComparison;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.foundationdb.qp.operator.API.delete_Returning;
import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.insert_Returning;
import static com.foundationdb.qp.operator.API.update_Returning;
import static com.foundationdb.qp.operator.API.valuesScan_Default;
import static org.junit.Assert.assertEquals;

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
        tID = createTable(SCHEMA, TABLE, "id INT NOT NULL PRIMARY KEY, x INT, y INT");
        createIndex(SCHEMA, TABLE, "x", "x");
        tableRowType = SchemaCache.globalSchema(ais()).tableRowType(tID);
        pkID = tableRowType.table().getPrimaryKey().getIndex().getIndexId();
        xID = tableRowType.table().getIndex("x").getIndexId();
        writeRows(createNewRow(tID, 3, 30, 300),
                  createNewRow(tID, 4, 40, 400),
                  createNewRow(tID, 6, 60, 600),
                  createNewRow(tID, 7, 70, 700));
        groupRows = runPlanTxn(groupScanCreator());
        pkRows = runPlanTxn(indexScanCreator(pkID));
        indexRows = runPlanTxn(indexScanCreator(xID));
    }

    //
    // Insert
    //

    @Test
    public void groupScanAndInsert() {
        Row row = testRow(tableRowType, 5, 50, 500);
        test(groupScanCreator(), insertCreator(row), groupRows, combine(groupRows, row));
    }

    @Test
    public void pkScanAndInsert() {
        Row row = testRow(tableRowType, 5, 50, 500);
        Row pkRow = testRow(tableRowType.indexRowType(pkID), 5);
        test(indexScanCreator(pkID), insertCreator(row), pkRows, combine(pkRows, pkRow));
    }

    @Test
    public void indexScanAndInsert() {
        Row row = testRow(tableRowType, 5, 50, 500);
        Row xRow = testRow(tableRowType.indexRowType(xID), 50, 5);
        test(indexScanCreator(xID), insertCreator(row), indexRows, combine(indexRows, xRow));
    }

    //
    // Update
    //

    @Test
    public void groupScanAndUpdate() {
        Row oldRow = groupRows.get(0);
        Row newRow = testRow(tableRowType, 3, 30, 301); // Does not affect position
        test(groupScanCreator(), updateCreator(oldRow, newRow), groupRows, replace(groupRows, 0, newRow));
    }

    @Test
    public void pkScanAndUpdate() {
        Row oldRow = groupRows.get(0);
        Row newRow = testRow(tableRowType, 1, 30, 300); // Moves row prior to first
        Row newPKRow = testRow(tableRowType.indexRowType(pkID), 1);
        test(indexScanCreator(pkID), updateCreator(oldRow, newRow), pkRows, replace(pkRows, 0, newPKRow));
    }

    @Test
    public void indexScanAndUpdate() {
        Row oldRow = groupRows.get(0);
        Row newRow = testRow(tableRowType, 3, 300, 300); // Moves row after last
        Row newXRow = testRow(tableRowType.indexRowType(xID), 300, 3);
        test(indexScanCreator(xID), updateCreator(oldRow, newRow), indexRows, replace(indexRows, 0, newXRow));
    }

    //
    // Delete
    //

    @Test
    public void groupScanAndDelete() {
        Row row = groupRows.get(0);
        test(groupScanCreator(), deleteCreator(row), groupRows, groupRows.subList(1, groupRows.size()));
    }

    @Test
    public void pkScanAndDelete() {
        Row row = groupRows.get(0);
        test(indexScanCreator(pkID), deleteCreator(row), pkRows, pkRows.subList(1, pkRows.size()));
    }

    @Test
    public void indexScanAndDelete() {
        Row row = groupRows.get(0);
        test(indexScanCreator(xID), deleteCreator(row), indexRows, indexRows.subList(1, indexRows.size()));
    }

    //
    // Internal
    //

    private OperatorCreator groupScanCreator() {
        return new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                return groupScan_Default(schema.tableRowType(tID).table().getGroup());
            }
        };
    }

    private OperatorCreator indexScanCreator(final int iID) {
        return new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                return API.indexScan_Default(schema.tableRowType(tID).indexRowType(iID));
            }
        };
    }

    private OperatorCreator insertCreator(final Row newRow) {
        return new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                RowType rowType = schema.tableRowType(tID);
                return insert_Returning(valuesScan_Default(bindableRows(newRow), rowType));
            }
        };
    }

    private OperatorCreator updateCreator(final Row oldRow, final Row newRow) {
        return new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                RowType rowType = schema.tableRowType(tID);
                return update_Returning(
                    valuesScan_Default(bindableRows(oldRow), rowType),
                    new UpdateFunction() {
                        @Override
                        public Row evaluate(Row original, QueryContext context, QueryBindings bindings) {
                            return newRow;
                        }

                        @Override
                        public boolean rowIsSelected(Row row) {
                            return row.value(0).getInt32() == oldRow.value(0).getInt32();
                        }
                    });
            }
        };
    }

    private OperatorCreator deleteCreator(final Row row) {
        return new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                RowType rowType = schema.tableRowType(tID);
                return delete_Returning(valuesScan_Default(bindableRows(row), rowType), false);
            }
        };
    }

    private void test(OperatorCreator scan, OperatorCreator dml, List<Row> startingRows, List<Row> finalRows) {
        List<MonitoredOperatorThread> threads = readAndDML(scan, dml);
        assertEquals("dml scan size", 1, threads.get(1).getScannedRows().size());
        compareRows(startingRows, threads.get(0).getScannedRows());
        compareRows(finalRows, runPlanTxn(scan));
    }

    /**  Plan that ensures reader starts transaction first but doesn't perform any reads until write is finished. */
    private List<MonitoredOperatorThread> readAndDML(OperatorCreator readPlan, OperatorCreator dmlPlan) {
        List<MonitoredOperatorThread> threads = ConcurrentTestBuilderImpl
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

    //
    // Static
    //

    private static List<Row> combine(List<Row> rows, Row newRow) {
        List<Row> newRows = new ArrayList<>(rows);
        newRows.add(newRow);
        Collections.sort(newRows, ROW_COMPARATOR);
        return newRows;
    }

    private static List<Row> replace(List<Row> rows, int index, Row newRow) {
        List<Row> newRows = new ArrayList<>(rows);
        newRows.set(index, newRow);
        Collections.sort(newRows, ROW_COMPARATOR);
        return newRows;
    }

    private static final Comparator<Row> ROW_COMPARATOR = new Comparator<Row>() {
        @Override
        public int compare(Row r1, Row r2) {
            Integer i1 = r1.value(0).getInt32();
            Integer i2 = r2.value(0).getInt32();
            return i1.compareTo(i2);
        }
    };
}
