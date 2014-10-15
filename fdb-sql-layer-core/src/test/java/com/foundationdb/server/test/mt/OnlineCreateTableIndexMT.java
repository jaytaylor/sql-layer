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
import com.foundationdb.ais.model.Index;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.API.SortOption;
import com.foundationdb.qp.operator.ExpressionGenerator;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.test.ExpressionGenerators;
import com.foundationdb.server.test.mt.util.OperatorCreator;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertNotNull;

/** Interleaved DML during an online create index for a single table. */
public class OnlineCreateTableIndexMT extends OnlineMTBase
{
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final String INDEX_NAME = "x";
    private static final String CREATE_INDEX = "CREATE INDEX x ON "+TABLE+"("+ INDEX_NAME +")";

    private int tID;
    TableRowType tableRowType;
    List<Row> groupRows;

    @Before
    public void createAndLoad() {
        tID = createTable(SCHEMA, TABLE, "id INT NOT NULL PRIMARY KEY, x INT");
        tableRowType = SchemaCache.globalSchema(ais()).tableRowType(tID);
        writeRows(createNewRow(tID, 2, 20),
                  createNewRow(tID, 4, 40));
        groupRows = runPlanTxn(groupScanCreator(tID));
    }


    @Override
    protected String getDDL() {
        return CREATE_INDEX;
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
        // Generate what should be in the table index from the group rows
        return runPlanTxn(new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                RowType tType = schema.tableRowType(tID);
                List<ExpressionGenerator> expList = Arrays.asList(
                    ExpressionGenerators.field(tableRowType, 0, 1), // x
                    ExpressionGenerators.field(tableRowType, 1, 0)  // id
                );
                Ordering ordering = API.ordering();
                for(int i = 0; i < expList.size(); ++i) {
                    TPreparedExpression prep = expList.get(i).getTPreparedExpression();
                    ordering.append(ExpressionGenerators.field(prep.resultType(), i), true);
                }
                Operator plan = API.groupScan_Default(tType.table().getGroup());
                plan = API.project_Default(plan, expList, tType);
                plan = API.sort_General(plan, plan.rowType(), ordering, SortOption.PRESERVE_DUPLICATES);
                return plan;
            }
        });
    }

    @Override
    protected OperatorCreator getGroupCreator() {
        return groupScanCreator(tID);
    }

    @Override
    protected OperatorCreator getOtherCreator() {
        return indexScanCreator(tID, INDEX_NAME);
    }

    @Override
    protected void postCheckAIS(AkibanInformationSchema ais) {
        Index newIndex = ais().getTable(tID).getIndex(INDEX_NAME);
        assertNotNull("new index", newIndex);
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
        dmlPostMetaToPreFinal(insertCreator(tID, newRow), combine(groupRows, newRow));
    }

    @Test
    public void updatePostMetaToPreFinal() {
        Row oldRow = testRow(tableRowType, 2, 20);
        Row newRow = testRow(tableRowType, 2, 21);
        dmlPostMetaToPreFinal(updateCreator(tID, oldRow, newRow), replace(groupRows, 0, newRow));
    }

    @Test
    public void deletePostMetaToPreFinal() {
        Row oldRow = groupRows.get(0);
        dmlPostMetaToPreFinal(deleteCreator(tID, oldRow), groupRows.subList(1, groupRows.size()));
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
}
