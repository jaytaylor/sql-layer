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
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import java.util.Arrays;


/** Interleaved DML during an online create index for a single table. */
public class OnlineCreateTableAsMT extends OnlineCreateTableAsBase {

    @Before
    public void createAndLoad() {
        CREATE_QUERY = " CREATE TABLE " + TO_TABLE + " AS SELECT * FROM " + FROM_TABLE + " WITH DATA ";
        ftID = createTable(SCHEMA_NAME, FROM_TABLE, "id INT NOT NULL PRIMARY KEY, x INT");

        fromTableRowType = SchemaCache.globalSchema(ais()).tableRowType(ftID);

        writeRows(createNewRow(ftID, -1, 10),
                createNewRow(ftID, 2, 20),
                createNewRow(ftID, 3, 30),
                createNewRow(ftID, 4, 40));

        fromGroupRows = runPlanTxn(groupScanCreator(ftID));//runs given plan and returns output row
        columnNames = Arrays.asList("id", "x");
        DataTypeDescriptor d = new DataTypeDescriptor(TypeId.INTEGER_ID, false);
        fromDescriptors = Arrays.asList(d, d);
        server = new TestSession();
    }

    //
    // I/U/D pre-to-post METADATA
    //
    @Test
    public void insertPreToPostMetadata() {
        Row newRow = testRow(fromTableRowType, 5, 50);
        otherGroupRows = getGroupExpected();
        dmlPreToPostMetadata(insertCreator(ftID, newRow), getGroupExpected(), true, fromDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Test
    public void updatePreToPostMetadata() {
        Row oldRow = testRow(fromTableRowType, 2, 20);
        Row newRow = testRow(fromTableRowType, 2, 21);
        otherGroupRows = getGroupExpected();
        dmlPreToPostMetadata(updateCreator(ftID, oldRow, newRow), getGroupExpected(), true, fromDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Test
    public void deletePreToPostMetadata() {
        Row oldRow = fromGroupRows.get(0);
        otherGroupRows = getGroupExpected();
        dmlPreToPostMetadata(deleteCreator(ftID, oldRow), getGroupExpected(), true, fromDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    //
    // I/U/D post METADATA to pre FINAL
    //
    @Test
    public void insertPostMetaToPreFinal() {
        Row newRow = testRow(fromTableRowType, 5, 50);
        otherGroupRows = combine(fromGroupRows, newRow);
        dmlPostMetaToPreFinal(insertCreator(ftID, newRow), combine(fromGroupRows, newRow), true, true, fromDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Ignore
    public void updatePostMetaToPreFinal() {
        Row oldRow = testRow(fromTableRowType, 2, 20);
        Row newRow = testRow(fromTableRowType, 2, 21);
        otherGroupRows = replace(fromGroupRows, 1, newRow);
        dmlPostMetaToPreFinal(updateCreator(ftID, oldRow, newRow), replace(fromGroupRows, 1, newRow), true, true, fromDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Ignore
    public void deletePostMetaToPreFinal() {
        Row oldRow = fromGroupRows.get(0);
        otherGroupRows = fromGroupRows.subList(1, fromGroupRows.size());
        dmlPostMetaToPreFinal(deleteCreator(ftID, oldRow), fromGroupRows.subList(1, fromGroupRows.size()), true, true, fromDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    //
    // I/U/D pre-to-post FINAL
    //
    @Test
    public void insertPreToPostFinal() {
        Row newRow = testRow(fromTableRowType, 5, 50);
        otherGroupRows = getGroupExpected();
        dmlPreToPostFinal(insertCreator(ftID, newRow), getGroupExpected(), true, fromDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Test
    public void updatePreToPostFinal() {
        Row oldRow = testRow(fromTableRowType, 2, 20);
        Row newRow = testRow(fromTableRowType, 2, 21);
        otherGroupRows = getGroupExpected();
        dmlPreToPostFinal(updateCreator(ftID, oldRow, newRow), getGroupExpected(), true, fromDescriptors, columnNames, server, CREATE_QUERY, true);
    }

    @Test
    public void deletePreToPostFinal() {
        Row oldRow = fromGroupRows.get(0);
        otherGroupRows = getGroupExpected();
        dmlPreToPostFinal(deleteCreator(ftID, oldRow), getGroupExpected(), true, fromDescriptors, columnNames, server, CREATE_QUERY, true);
    }
}