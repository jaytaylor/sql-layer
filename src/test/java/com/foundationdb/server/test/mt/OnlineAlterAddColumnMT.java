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
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.server.types.TInstance;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;

/** Interleaved DML during an online column addition. */
public class OnlineAlterAddColumnMT extends OnlineMTBase
{
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final String COLUMN_NAME = "y";
    private static final String ALTER_ADD_COLUMN = "ALTER TABLE "+TABLE+" ADD COLUMN "+COLUMN_NAME+" INT";

    private int tID;
    private TableRowType tableRowType;
    private RowType newRowType;
    private List<Row> groupRows;

    @Before
    public void createAndLoad() {
        tID = createTable(SCHEMA, TABLE, "id INT NOT NULL PRIMARY KEY, x INT");
        tableRowType = SchemaCache.globalSchema(ais()).tableRowType(tID);
        writeRows(createNewRow(tID, 2, 20),
                  createNewRow(tID, 4, 40));
        groupRows = runPlanTxn(groupScanCreator(tID));

        TInstance type = tableRowType.typeAt(1);
        newRowType = SchemaCache.globalSchema(ais()).newValuesType(type, type, type);
    }


    @Override
    protected String getDDL() {
        return ALTER_ADD_COLUMN;
    }

    @Override
    protected String getDDLSchema() {
        return SCHEMA;
    }

    @Override
    protected List<Row> getGroupExpected() {
        return getRowsWithNull(groupRows);
    }

    @Override
    protected List<Row> getOtherExpected() {
        return null;
    }

    @Override
    protected OperatorCreator getGroupCreator() {
        return groupScanCreator(tID);
    }

    @Override
    protected OperatorCreator getOtherCreator() {
        return null;
    }

    @Override
    protected void postCheckAIS(AkibanInformationSchema ais) {
        Table table = ais.getTable(tID);
        Column column = table.getColumn(COLUMN_NAME);
        assertNotNull("new column present", column);
    }


    //
    // I/U/D pre-to-post METADATA
    //

    @Test
    public void insertPreToPostMetadata() {
        Row newRow = testRow(tableRowType, 5, 50);
        dmlPreToPostMetadata(insertCreator(tID, newRow));
    }

    @Test
    public void updatePreToPostMetadata() {
        Row oldRow = testRow(tableRowType, 2, 20);
        Row newRow = testRow(tableRowType, 2, 21);
        dmlPreToPostMetadata(updateCreator(tID, oldRow, newRow));
    }

    @Test
    public void deletePreToPostMetadata() {
        Row oldRow = groupRows.get(0);
        dmlPreToPostMetadata(deleteCreator(tID, oldRow));
    }

    //
    // I/U/D post METADATA to pre FINAL
    //

    @Test
    public void insertPostMetaToPreFinal() {
        Row newRow = testRow(tableRowType, 5, 50);
        dmlPostMetaToPreFinal(insertCreator(tID, newRow), getRowsWithNull(combine(groupRows, newRow)));
    }

    @Test
    public void updatePostMetaToPreFinal() {
        Row oldRow = testRow(tableRowType, 2, 20);
        Row newRow = testRow(tableRowType, 2, 21);
        dmlPostMetaToPreFinal(updateCreator(tID, oldRow, newRow), getRowsWithNull(replace(groupRows, 0, newRow)));
    }

    @Test
    public void deletePostMetaToPreFinal() {
        Row oldRow = groupRows.get(0);
        dmlPostMetaToPreFinal(deleteCreator(tID, oldRow), getRowsWithNull(groupRows.subList(1, groupRows.size())));
    }

    //
    // I/U/D pre-to-post FINAL
    //

    @Test
    public void insertPreToPostFinal() {
        Row newRow = testRow(tableRowType, 5, 50);
        dmlPreToPostFinal(insertCreator(tID, newRow));
    }

    @Test
    public void updatePreToPostFinal() {
        Row oldRow = testRow(tableRowType, 2, 20);
        Row newRow = testRow(tableRowType, 2, 21);
        dmlPreToPostFinal(updateCreator(tID, oldRow, newRow));
    }

    @Test
    public void deletePreToPostFinal() {
        Row oldRow = groupRows.get(0);
        dmlPreToPostFinal(deleteCreator(tID, oldRow));
    }

    private List<Row> getRowsWithNull(List<Row> rows) {
        List<Row> newRows = new ArrayList<>();
        for(Row row : rows) {
            newRows.add(testRow(newRowType, row.value(0).getInt32(), row.value(1).getInt32(), null));
        }
        return newRows;
    }
}
