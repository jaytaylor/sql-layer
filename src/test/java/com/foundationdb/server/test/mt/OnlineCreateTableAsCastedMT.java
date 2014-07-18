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
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.mt.util.OnlineCreateTableAsBase;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import java.util.Arrays;


/** Interleaved DML during an online create index for a single table. */
public class OnlineCreateTableAsCastedMT extends OnlineCreateTableAsBase {

    private String NEW_TABLE = "nt";

    @Before
    public void createAndLoad() {
        CREATE_QUERY = " CREATE TABLE " + NEW_TABLE + " AS SELECT CAST( ABS(id) AS DOUBLE ) , x IS NOT NULL FROM " + FROM_TABLE + " WITH DATA ";
        ftID = createTable(SCHEMA, FROM_TABLE, "id INT NOT NULL PRIMARY KEY, x INT");
        ttID = createTable(SCHEMA, TO_TABLE, "id INT NOT NULL PRIMARY KEY, x BOOLEAN");

        fromTableRowType = SchemaCache.globalSchema(ais()).tableRowType(ftID);
        toTableRowType = SchemaCache.globalSchema(ais()).tableRowType(ttID);

        writeRows(createNewRow(ftID, -1, 10),
                createNewRow(ftID, 2, 20),
                createNewRow(ftID, 3, 30),
                createNewRow(ftID, 4, 40),
                createNewRow(ttID, 1, true),
                createNewRow(ttID, 2, true),
                createNewRow(ttID, 3, true),
                createNewRow(ttID, 4, true));

        fromGroupRows = runPlanTxn(groupScanCreator(ftID));//runs given plan and returns output row
        toGroupRows = runPlanTxn(groupScanCreator(ttID));
        columnNames = Arrays.asList("id", "x");
        DataTypeDescriptor d = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
        DataTypeDescriptor cd = new DataTypeDescriptor(TypeId.BOOLEAN_ID, false);
        toDescriptors = Arrays.asList(d, cd);
        server = new TestSession();
    }

    @Override
    protected OperatorCreator getOtherCreator() {
        int temp = ais().getTable(SCHEMA, NEW_TABLE).getTableId();
        return groupScanCreator(temp);
    }

    //
    // I/U/D pre-to-post METADATA
    //

    @Test
    public void InsertPreToPostMetadata() {
        Row newRow = testRow(fromTableRowType, 5, 50);
        otherGroupRows = getToExpected();
        dmlPreToPostMetadata(insertCreator(ftID, newRow), getGroupExpected(), true, toDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Test
    public void UpdatePreToPostMetadata() {
        Row oldRow = testRow(fromTableRowType, 2, 20);
        Row newRow = testRow(fromTableRowType, 2, 21);
        otherGroupRows = getToExpected();
        dmlPreToPostMetadata(updateCreator(ftID, oldRow, newRow), getGroupExpected(), true, toDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Test
    public void DeletePreToPostMetadata() {
        Row oldRow = fromGroupRows.get(0);
        otherGroupRows = getToExpected();
        dmlPreToPostMetadata(deleteCreator(ftID, oldRow), getGroupExpected(), true, toDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    //
    // I/U/D post METADATA to pre FINAL
    //

    @Test
    public void InsertPostMetaToPreFinal() {
        Row newRow = testRow(fromTableRowType, 5, 50);
        Row newCastRow = testRow(toTableRowType, 5, true);
        otherGroupRows = combine(getToExpected(), newCastRow);
        dmlPostMetaToPreFinal(insertCreator(ftID, newRow), combine(fromGroupRows, newRow), true, true, toDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Ignore
    public void UpdatePostMetaToPreFinal() {
        Row oldRow = testRow(fromTableRowType, 2, 20);
        Row newRow = testRow(fromTableRowType, 2, 21);
        otherGroupRows = getToExpected();
        dmlPostMetaToPreFinal(updateCreator(ftID, oldRow, newRow), replace(fromGroupRows, 1, newRow), true, true, toDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Ignore
    public void DeletePostMetaToPreFinal() {
        Row oldRow = fromGroupRows.get(0);
        otherGroupRows = toGroupRows.subList(1, toGroupRows.size());
        dmlPostMetaToPreFinal(deleteCreator(ftID, oldRow), fromGroupRows.subList(1, fromGroupRows.size()), true, true, toDescriptors, columnNames, server, CREATE_QUERY, true);
    }
    //
    // I/U/D pre-to-post FINAL
    //

    @Test
    public void InsertPreToPostFinal() {
        Row newRow = testRow(fromTableRowType, 5, 50);
        otherGroupRows = getToExpected();
        dmlPreToPostFinal(insertCreator(ftID, newRow), getGroupExpected(), true, toDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Test
    public void UpdatePreToPostFinal() {
        Row oldRow = testRow(fromTableRowType, 2, 20);
        Row newRow = testRow(fromTableRowType, 2, 21);
        otherGroupRows = getToExpected();
        dmlPreToPostFinal(updateCreator(ftID, oldRow, newRow), getGroupExpected(), true, toDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Test
    public void DeletePreToPostFinal() {
        Row oldRow = fromGroupRows.get(0);
        otherGroupRows = getToExpected();
        dmlPreToPostFinal(deleteCreator(ftID, oldRow), getGroupExpected(), true, toDescriptors, columnNames, server, CREATE_QUERY, true);
    }
}