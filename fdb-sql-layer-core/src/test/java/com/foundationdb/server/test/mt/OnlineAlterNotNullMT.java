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
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/** Interleaved DML during an online column null to not-null change. */
public class OnlineAlterNotNullMT extends OnlineMTBase
{
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final String COLUMN = "x";
    private static final String ALTER_NOT_NULL = "ALTER TABLE "+TABLE+" ALTER COLUMN "+COLUMN+" NOT NULL";

    private int tID;
    private TableRowType tableRowType;
    private List<Row> groupRows;
    private boolean expectedNullable;

    @Before
    public void createAndLoad() {
        tID = createTable(SCHEMA, TABLE, "id INT NOT NULL PRIMARY KEY, x INT");
        tableRowType = SchemaCache.globalSchema(ais()).tableRowType(tID);
        writeRows(row(tID, 2, 20),
                  row(tID, 4, 40));
        groupRows = runPlanTxn(groupScanCreator(tID));
        expectedNullable = false;
    }


    @Override
    protected String getDDL() {
        return ALTER_NOT_NULL;
    }

    @Override
    protected String getDDLSchema() {
        return SCHEMA;
    }

    @Override
    protected List<Row> getGroupExpected() {
        return groupRows;
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
        Table table = ais.getTable(SCHEMA, TABLE);
        Column column = table.getColumn(COLUMN);
        assertEquals("column nullable", expectedNullable, column.getNullable());
    }

    //
    // I/U pre-to-post METADATA
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

    //
    // I/U post METADATA to pre FINAL
    //

    @Test
    public void insertPostMetaToPreFinal() {
        Row newRow = testRow(tableRowType, 5, 50);
        dmlPostMetaToPreFinal(insertCreator(tID, newRow), combine(groupRows, newRow));
    }

    @Test
    public void updatePostMetaToPreFinal() {
        Row oldRow = testRow(tableRowType, 2, 20);
        Row newRow = testRow(tableRowType, 2, 21);
        dmlPostMetaToPreFinal(updateCreator(tID, oldRow, newRow), replace(groupRows, 0, newRow));
    }

    @Test
    public void insertViolationPostMetaToPreFinal() {
        expectedNullable = true;
        Row newRow = testRow(tableRowType, 5, null);
        dmlViolationPostMetaToPreFinal(insertCreator(tID, newRow), combine(groupRows, newRow));
    }

    @Test
    public void updateViolationPostMetaToPreFinal() {
        expectedNullable = true;
        Row oldRow = testRow(tableRowType, 2, 20);
        Row newRow = testRow(tableRowType, 2, null);
        dmlViolationPostMetaToPreFinal(updateCreator(tID, oldRow, newRow), replace(groupRows, 0, newRow));
    }

    //
    // I/U pre-to-post FINAL
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
}
