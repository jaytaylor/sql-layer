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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.ForeignKey;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.service.dxl.OnlineDDLMonitor;
import com.foundationdb.server.test.mt.util.ConcurrentTestBuilderImpl;
import com.foundationdb.server.test.mt.util.MonitoredThread;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import com.foundationdb.server.test.mt.util.ThreadHelper.UncaughtHandler;
import com.foundationdb.server.test.mt.util.ThreadMonitor;
import com.foundationdb.server.test.mt.util.TimeMarkerComparison;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Interleaved DML during an ALTER ADD FOREIGN KEY. */
public class OnlineAlterAddForeignKeyMT extends OnlineMTBase
{
    private static final String SCHEMA = "test";
    private static final String PARENT_TABLE = "p";
    private static final String CHILD_TABLE = "c";
    private static final String ALTER_ADD_FK = "ALTER TABLE "+CHILD_TABLE+" ADD CONSTRAINT fk1 FOREIGN KEY(pid) REFERENCES p(pid)";

    private int pID, cID;
    private TableRowType parentRowType, childRowType;
    private List<Row> parentGroupRows, childGroupRows;

    @Before
    public void createAndLoad() {
        pID = createTable(SCHEMA, PARENT_TABLE, "pid INT NOT NULL PRIMARY KEY");
        cID = createTable(SCHEMA, CHILD_TABLE, "cid INT NOT NULL PRIMARY KEY, pid INT");
        parentRowType = SchemaCache.globalSchema(ais()).tableRowType(pID);
        childRowType = SchemaCache.globalSchema(ais()).tableRowType(cID);
        writeRows(createNewRow(pID, 1),
                  createNewRow(pID, 2));
        writeRows(createNewRow(cID, 10, 1),
                  createNewRow(cID, 20, 2));
        parentGroupRows = runPlanTxn(groupScanCreator(pID));
        childGroupRows = runPlanTxn(groupScanCreator(cID));
    }


    @Override
    protected String getDDL() {
        return ALTER_ADD_FK;
    }

    @Override
    protected String getDDLSchema() {
        return SCHEMA;
    }

    @Override
    protected List<Row> getGroupExpected() {
        return childGroupRows;
    }

    @Override
    protected List<Row> getOtherExpected() {
        return null;
    }

    @Override
    protected OperatorCreator getGroupCreator() {
        return groupScanCreator(cID);
    }

    @Override
    protected OperatorCreator getOtherCreator() {
        return null;
}

    @Override
    protected void postCheckAIS(AkibanInformationSchema ais) {
        Table table = ais.getTable(SCHEMA, CHILD_TABLE);
        List<String> fks = new ArrayList<>();
        for(ForeignKey f : table.getReferencingForeignKeys()) {
            fks.add(f.getConstraintName());
        }
        assertEquals("[fk1]", fks.toString());
    }


    //
    // I/U pre-to-post METADATA
    //

    @Test
    public void insertPreToPostMetadata_Child() {
        Row newRow = testRow(childRowType, 100, 10);
        dmlPreToPostMetadata(insertCreator(cID, newRow));
    }

    @Test
    public void updatePreToPostMetadata_Child() {
        Row oldRow = testRow(childRowType, 20, 2);
        Row newRow = testRow(childRowType, 20, 20);
        dmlPreToPostMetadata(updateCreator(cID, oldRow, newRow));
    }

    @Test
    public void updatePreToPostMetadata_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        Row newRow = testRow(parentRowType, 3);
        dmlViolationPreToPostMetadata_Parent(updateCreator(pID, oldRow, newRow), replace(parentGroupRows, 1, newRow));
    }

    @Test
    public void deletePreToPostMetadata_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        dmlViolationPreToPostMetadata_Parent(deleteCreator(pID, oldRow), remove(parentGroupRows, 1));
    }

    //
    // I/U post METADATA to pre FINAL
    //

    @Test
    public void insertPostMetaToPreFinal_Child() {
        Row newRow = testRow(childRowType, 21, 2);
        dmlPostMetaToPreFinal(insertCreator(cID, newRow), combine(childGroupRows, newRow));
    }

    @Test
    public void updatePostMetaToPreFinal_Child() {
        Row oldRow = testRow(childRowType, 10, 1);
        Row newRow = testRow(childRowType, 10, 2);
        dmlPostMetaToPreFinal(updateCreator(cID, oldRow, newRow), replace(childGroupRows, 0, newRow));
    }

    @Test
    public void insertViolationPostMetaToPreFinal_Child() {
        Row newRow = testRow(childRowType, 100, 10);
        dmlViolationPostMetaToPreFinal(insertCreator(cID, newRow), childGroupRows, true);
    }

    @Test
    public void updateViolationPostMetaToPreFinal_Child() {
        Row oldRow = testRow(childRowType, 20, 2);
        Row newRow = testRow(childRowType, 20, 20);
        dmlViolationPostMetaToPreFinal(updateCreator(cID, oldRow, newRow), childGroupRows, true);
    }

    @Test
    public void updateViolationPostMetaToPreFinal_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        Row newRow = testRow(parentRowType, 3);
        dmlViolationPostMetaToPreFinal(updateCreator(pID, oldRow, newRow), parentGroupRows, false);
    }

    @Test
    public void deleteViolationPostMetaToPreFinal_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        dmlViolationPostMetaToPreFinal(deleteCreator(pID, oldRow), parentGroupRows, false);
    }

    //
    // I/U pre-to-post FINAL
    //

    @Test
    public void insertPreToPostFinal_Child() {
        Row newRow = testRow(childRowType, 21, 2);
        dmlPreToPostFinal(insertCreator(cID, newRow));
    }

    @Test
    public void updatePreToPostFinal_Child() {
        Row oldRow = testRow(childRowType, 10, 1);
        Row newRow = testRow(childRowType, 10, 2);
        dmlPreToPostFinal(updateCreator(cID, oldRow, newRow));
    }

    @Test
    public void updatePreToPostFinal_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        Row newRow = testRow(parentRowType, 3);
        dmlPreToPostFinal(updateCreator(pID, oldRow, newRow));
    }

    @Test
    public void deletePreToPostFinal_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        dmlPreToPostFinal(deleteCreator(pID, oldRow));
    }


    private void dmlViolationPreToPostMetadata_Parent(OperatorCreator dmlCreator, List<Row> dmlSuccessParentRows) {
        // dmlPreToPostMetadata is deterministic for child but not parent.
        // DDL will fail if DML finishes first and vice-versa
        List<MonitoredThread> threads = dmlPreToPostMetadata_Build(dmlCreator);
        UncaughtHandler handler = ThreadHelper.startAndJoin(threads);
        if(handler.thrown.containsKey(threads.get(0))) {
            new TimeMarkerComparison(threads).verify("DML:PRE_BEGIN",
                                                     "DDL:PRE_METADATA",
                                                     "DDL:POST_METADATA",
                                                     "DML:PRE_COMMIT",
                                                     "DDL:ForeignKeyReferencingViolationException");
            checkExpectedRows(dmlSuccessParentRows, groupScanCreator(pID));
            checkExpectedRows(childGroupRows, groupScanCreator(cID));
        } else {
            new TimeMarkerComparison(threads).verify("DML:PRE_BEGIN",
                                                     "DDL:PRE_METADATA",
                                                     "DDL:POST_METADATA",
                                                     "DML:PRE_COMMIT",
                                                     "DML:TableVersionChangedException");
            checkExpectedRows(parentGroupRows, groupScanCreator(pID));
            checkExpectedRows(childGroupRows, groupScanCreator(cID));
        }
    }

    private void dmlViolationPostMetaToPreFinal(OperatorCreator dmlCreator, List<Row> finalGroupRows, boolean isChild) {
        List<MonitoredThread> threads = ConcurrentTestBuilderImpl
            .create()
            .add("DDL", getDDLSchema(), getDDL())
            .sync("a", OnlineDDLMonitor.Stage.PRE_TRANSFORM)
            .sync("b", OnlineDDLMonitor.Stage.PRE_FINAL)
            .mark(OnlineDDLMonitor.Stage.POST_METADATA, OnlineDDLMonitor.Stage.PRE_FINAL)
            .add("DML", dmlCreator)
            .sync("a", ThreadMonitor.Stage.PRE_BEGIN)
            .sync("b", ThreadMonitor.Stage.FINISH)
            .mark(ThreadMonitor.Stage.PRE_BEGIN)
            .build(this);
        ThreadHelper.startAndJoin(threads);
        new TimeMarkerComparison(threads).verify("DDL:POST_METADATA",
                                                 "DML:PRE_BEGIN",
                                                 "DML:ForeignKeyReferenc" + (isChild ? "ing" : "ed") + "ViolationException",
                                                 "DDL:PRE_FINAL");
        checkExpectedRows(finalGroupRows, groupScanCreator(isChild ? cID : pID));
    }
}
