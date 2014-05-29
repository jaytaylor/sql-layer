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
import com.foundationdb.server.error.ForeignKeyReferencedViolationException;
import com.foundationdb.server.error.ForeignKeyReferencingViolationException;
import com.foundationdb.server.rowdata.SchemaFactory;
import com.foundationdb.server.test.mt.util.MonitoredThread;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.server.test.mt.util.ThreadHelper;
import com.foundationdb.server.test.mt.util.ThreadHelper.UncaughtHandler;
import com.foundationdb.server.test.mt.util.TimeMarkerComparison;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Interleaved DML during an ALTER ADD FOREIGN KEY. */
public class OnlineAlterAddForeignKeyMT extends OnlineMTBase
{
    protected static final String SCHEMA = "test";
    protected static final String PARENT_TABLE = "p";
    protected static final String CHILD_TABLE = "c";
    protected static final String ALTER_ADD_FK = "ALTER TABLE "+CHILD_TABLE+" ADD CONSTRAINT fk1 FOREIGN KEY(pid) "+
                                                 "REFERENCES "+PARENT_TABLE+"(pid)";

    protected int pID, cID;
    protected TableRowType parentRowType, childRowType;
    protected List<Row> parentGroupRows, childGroupRows;
    protected Class<? extends Exception> failingExceptionClass;

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
        failingExceptionClass = null;
    }

    @Override
    protected Class<? extends Exception> getFailingExceptionClass() {
        return (failingExceptionClass != null) ? failingExceptionClass : super.getFailingExceptionClass();
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
            fks.add(f.getConstraintName().getTableName());
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
        dmlPreToPostMetadata(updateCreator(pID, oldRow, newRow));
    }

    @Test
    public void deletePreToPostMetadata_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        dmlPreToPostMetadata(deleteCreator(pID, oldRow));
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
        dmlPostMetaToPreFinal(insertCreator(cID, newRow), parentGroupRows, childGroupRows, true);
    }

    @Test
    public void updateViolationPostMetaToPreFinal_Child() {
        Row oldRow = testRow(childRowType, 20, 2);
        Row newRow = testRow(childRowType, 20, 20);
        dmlPostMetaToPreFinal(updateCreator(cID, oldRow, newRow), parentGroupRows, childGroupRows, true);
    }

    @Ignore("Broken!")
    @Test
    public void updateViolationPostMetaToPreFinal_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        Row newRow = testRow(parentRowType, 3);
        dmlPostMetaToPreFinal(updateCreator(pID, oldRow, newRow), parentGroupRows, childGroupRows, false);
    }

    @Ignore("Broken!")
    @Test
    public void deleteViolationPostMetaToPreFinal_Parent() {
        Row oldRow = testRow(parentRowType, 2);
        dmlPostMetaToPreFinal(deleteCreator(pID, oldRow), parentGroupRows, childGroupRows, false);
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

    protected void dmlPostMetaToPreFinal(OperatorCreator dmlCreator, List<Row> finalParentRows, List<Row> finalChildRows, Boolean isChildFailure) {
        if(isChildFailure == null) {
            failingExceptionClass = null;
        } else if(isChildFailure) {
            failingExceptionClass = ForeignKeyReferencingViolationException.class;
        } else {
            failingExceptionClass = ForeignKeyReferencedViolationException.class;
        }
        dmlPostMetaToPreFinal(dmlCreator, finalChildRows, failingExceptionClass == null);
        checkExpectedRows(finalParentRows, groupScanCreator(pID));
    }
}
