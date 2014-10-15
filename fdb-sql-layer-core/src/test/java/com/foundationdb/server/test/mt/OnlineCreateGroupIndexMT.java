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
import com.foundationdb.qp.operator.API.JoinType;
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

/** Interleaved DML during an online group index creation for a CAOI group. */
public class OnlineCreateGroupIndexMT extends OnlineMTBase
{
    private static final String SCHEMA = "test";
    private static final String C_TABLE = "c";
    private static final String A_TABLE = "a";
    private static final String O_TABLE = "o";
    private static final String I_TABLE = "i";
    private static final String INDEX_NAME = "gi_z_y_w";
    private static final String CREATE_INDEX = "CREATE INDEX "+INDEX_NAME+" ON i(i.z, o.y, c.w) USING LEFT JOIN";

    private int cID;
    private int aID;
    private int oID;
    private int iID;
    private TableRowType cRowType;
    private TableRowType aRowType;
    private TableRowType oRowType;
    private TableRowType iRowType;
    private List<Row> groupRows;

    @Before
    public void createAndLoad() {
        cID = createTable(SCHEMA, C_TABLE, "cid INT NOT NULL PRIMARY KEY, w INT");
        aID = createTable(SCHEMA, A_TABLE, "aid INT NOT NULL PRIMARY KEY, cid INT, x INT, GROUPING FOREIGN KEY(cid) REFERENCES c(cid)");
        oID = createTable(SCHEMA, O_TABLE, "oid INT NOT NULL PRIMARY KEY, cid INT, y INT, GROUPING FOREIGN KEY(cid) REFERENCES c(cid)");
        iID = createTable(SCHEMA, I_TABLE, "iid INT NOT NULL PRIMARY KEY, oid INT, z INT, GROUPING FOREIGN KEY(oid) REFERENCES o(oid)");
        Schema schema = SchemaCache.globalSchema(ais());
        cRowType = schema.tableRowType(cID);
        aRowType = schema.tableRowType(aID);
        oRowType = schema.tableRowType(oID);
        iRowType = schema.tableRowType(iID);
        writeRows(// unknown
                    // no O(65, 6)
                                createNewRow(iID, 650, 65, 650000),           // 0
                            createNewRow(cID, 2, 2000),                       // 1
                        createNewRow(aID, 20, 2, 20000),                      // 2
                                    createNewRow(oID, 25, 2, 25000),          // 3
                                createNewRow(iID, 250, 25, 250000),           // 4
                            createNewRow(cID, 4, 4000),                       // 5
                        // no A(40, 4, 40000)
                                    createNewRow(oID, 45, 4, 45000),          // 6
                                createNewRow(iID, 450, 45, 450000),           // 7
                            createNewRow(cID, 6, 6000),                       // 8
                        createNewRow(aID, 60, 6, 60000));                     // 9
        groupRows = runPlanTxn(groupScanCreator(cID));
    }

    @Override
    protected String getDDLSchema() {
        return SCHEMA;
    }

    @Override
    protected String getDDL() {
        return CREATE_INDEX;
    }

    @Override
    protected List<Row> getGroupExpected() {
        return groupRows;
    }

    @Override
    protected List<Row> getOtherExpected() {
        // Generate what should be in the group index from the group rows
        return runPlanTxn(new OperatorCreator() {
            @Override
            public Operator create(Schema schema) {
                RowType cType = schema.tableRowType(cID);
                RowType oType = schema.tableRowType(oID);
                RowType iType = schema.tableRowType(iID);
                List<ExpressionGenerator> expList = Arrays.asList(
                    ExpressionGenerators.field(iType, 2, 7), // z
                    ExpressionGenerators.field(oType, 2, 4), // y
                    ExpressionGenerators.field(cType, 1, 1), // w
                    ExpressionGenerators.field(cType, 0, 0), // cid
                    ExpressionGenerators.field(oType, 0, 2), // oid
                    ExpressionGenerators.field(iType, 0, 5)  // iid
                );
                Ordering ordering = API.ordering();
                for(int i = 0; i < expList.size(); ++i) {
                    TPreparedExpression prep = expList.get(i).getTPreparedExpression();
                    ordering.append(ExpressionGenerators.field(prep.resultType(), i), true);
                }
                Operator plan = API.groupScan_Default(cType.table().getGroup());
                plan = API.filter_Default(plan, Arrays.asList(cType, oType, iType));
                plan = API.flatten_HKeyOrdered(plan, cType, oType, JoinType.LEFT_JOIN);
                plan = API.flatten_HKeyOrdered(plan, plan.rowType(), iType, JoinType.LEFT_JOIN);
                plan = API.project_Default(plan, expList, plan.rowType());
                plan = API.sort_General(plan, plan.rowType(), ordering, SortOption.PRESERVE_DUPLICATES);
                return plan;
            }
        });
    }

    @Override
    protected OperatorCreator getGroupCreator() {
        return groupScanCreator(cID);
    }

    @Override
    protected OperatorCreator getOtherCreator() {
        return groupIndexScanCreator(cID, INDEX_NAME);
    }

    @Override
    protected void postCheckAIS(AkibanInformationSchema ais) {
        Index newIndex = ais.getTable(cID).getGroup().getIndex(INDEX_NAME);
        assertNotNull("new index", newIndex);
    }


    //
    // I/U/D pre-to-post METADATA
    //

    @Test
    public void insertPreToPostMetadata_C() {
        Row newRow = testRow(cRowType, 3, 3000);
        dmlPreToPostMetadata(insertCreator(cID, newRow));
    }

    @Test
    public void updatePreToPostMetadata_C() {
        Row oldRow = testRow(cRowType, 2, 2000);
        Row newRow = testRow(cRowType, 2, 2001);
        dmlPreToPostMetadata(updateCreator(cID, oldRow, newRow));
    }

    @Test
    public void deletePreToPostMetadata_C() {
        dmlPreToPostMetadata(deleteCreator(cID, groupRows.get(1)));
    }

    @Test
    public void insertPreToPostMetadata_A() {
        Row newRow = testRow(aRowType, 21, 2, 21000);
        dmlPreToPostMetadata(insertCreator(aID, newRow), insert(groupRows, 3, newRow), false);
    }

    @Test
    public void updatePreToPostMetadata_A() {
        Row oldRow = testRow(aRowType, 20, 2, 20000);
        Row newRow = testRow(aRowType, 20, 2, 20001);
        dmlPreToPostMetadata(updateCreator(aID, oldRow, newRow), replace(groupRows, 2, newRow), false);
    }

    @Test
    public void deletePreToPostMetadata_A() {
        Row oldRow = groupRows.get(2);
        dmlPreToPostMetadata(deleteCreator(aID, oldRow), remove(groupRows, oldRow), false);
    }

    @Test
    public void insertPreToPostMetadata_O() {
        Row newRow = testRow(oRowType, 26, 2, 26000);
        dmlPreToPostMetadata(insertCreator(oID, newRow));
    }

    @Test
    public void updatePreToPostMetadata_O() {
        Row oldRow = testRow(oRowType, 25, 2, 25000);
        Row newRow = testRow(oRowType, 25, 2, 25001);
        dmlPreToPostMetadata(updateCreator(oID, oldRow, newRow));
    }

    @Test
    public void deletePreToPostMetadata_O() {
        dmlPreToPostMetadata(deleteCreator(oID, groupRows.get(3)));
    }

    @Test
    public void insertPreToPostMetadata_I() {
        Row newRow = testRow(iRowType, 251, 25, 251000);
        dmlPreToPostMetadata(insertCreator(iID, newRow));
    }

    @Test
    public void updatePreToPostMetadata_I() {
        Row oldRow = testRow(iRowType, 250, 25, 250000);
        Row newRow = testRow(iRowType, 250, 25, 250001);
        dmlPreToPostMetadata(updateCreator(iID, oldRow, newRow));
    }

    @Test
    public void deletePreToPostMetadata_I() {
        dmlPreToPostMetadata(deleteCreator(iID, groupRows.get(4)));
    }

    //
    // I/U/D post METADATA to pre FINAL
    //

    @Test
    public void insertPostMetaToPreFinal_C() {
        Row newRow = testRow(cRowType, 5, 5000);
        dmlPostMetaToPreFinal(insertCreator(cID, newRow), insert(groupRows, 8, newRow));
    }

    @Test
    public void updatePostMetaToPreFinal_C() {
        Row oldRow = testRow(cRowType, 2, 2000);
        Row newRow = testRow(cRowType, 2, 2001);
        dmlPostMetaToPreFinal(updateCreator(cID, oldRow, newRow), replace(groupRows, 1, newRow));
    }

    @Test
    public void deletePostMetaToPreFinal_C() {
        Row oldRow = groupRows.get(1);
        dmlPostMetaToPreFinal(deleteCreator(cID, oldRow), remove(groupRows, oldRow));
    }

    @Test
    public void insertPostMetaToPreFinal_A() {
        Row newRow = testRow(aRowType, 40, 4, 40000);
        dmlPostMetaToPreFinal(insertCreator(aID, newRow), insert(groupRows, 6, newRow));
    }

    @Test
    public void updatePostMetaToPreFinal_A() {
        Row oldRow = testRow(aRowType, 20, 2, 20000);
        Row newRow = testRow(aRowType, 20, 2, 20001);
        dmlPostMetaToPreFinal(updateCreator(aID, oldRow, newRow), replace(groupRows, 2, newRow));
    }

    @Test
    public void deletePostMetaToPreFinal_A() {
        Row row = groupRows.get(2);
        dmlPostMetaToPreFinal(deleteCreator(aID, row), remove(groupRows, row));
    }

    @Test
    public void insertPostMetaToPreFinal_O() {
        Row newRow = testRow(oRowType, 65, 6, 6500);
        List<Row> expected = combine(groupRows, newRow);
        dmlPostMetaToPreFinal(insertCreator(oID, newRow), combine(expected, expected.remove(0)));
    }

    @Test
    public void updatePostMetaToPreFinal_O() {
        Row oldRow = testRow(oRowType, 25, 2, 25000);
        Row newRow = testRow(oRowType, 25, 2, 25001);
        dmlPostMetaToPreFinal(updateCreator(oID, oldRow, newRow), replace(groupRows, 3, newRow));
    }

    @Test
    public void deletePostMetaToPreFinal_O() {
        Row oldRow = groupRows.get(3);
        List<Row> expected = remove(groupRows, 3);
        dmlPostMetaToPreFinal(deleteCreator(oID, oldRow), insert(expected, 0, expected.remove(3)));
    }
    
    //
    // I/U/D pre-to-post FINAL
    //

    @Test
    public void insertPreToPostFinal_C() {
        Row newRow = testRow(cRowType, 5, 5000);
        dmlPreToPostFinal(insertCreator(cID, newRow));
    }

    @Test
    public void updatePreToPostFinal_C() {
        Row oldRow = testRow(cRowType, 2, 2000);
        Row newRow = testRow(cRowType, 2, 2001);
        dmlPreToPostFinal(updateCreator(cID, oldRow, newRow));
    }

    @Test
    public void deletePreToPostFinal_C() {
        Row oldRow = groupRows.get(1);
        dmlPreToPostFinal(deleteCreator(cID, oldRow));
    }

    @Test
    public void insertPreToPostFinal_A() {
        Row newRow = testRow(aRowType, 21, 2, 21000);
        dmlPreToPostFinal(insertCreator(aID, newRow), insert(groupRows, 3, newRow), false);
    }

    @Test
    public void updatePreToPostFinal_A() {
        Row oldRow = testRow(aRowType, 20, 2, 20000);
        Row newRow = testRow(aRowType, 20, 2, 20001);
        dmlPreToPostFinal(updateCreator(aID, oldRow, newRow), replace(groupRows, 2, newRow), false);
    }

    @Test
    public void deletePreToPostFinal_A() {
        Row row = groupRows.get(2);
        dmlPreToPostFinal(deleteCreator(aID, row), remove(groupRows, 2), false);
    }

    @Test
    public void insertPreToPostFinal_O() {
        Row newRow = testRow(oRowType, 26, 2, 26000);
        dmlPreToPostFinal(insertCreator(oID, newRow));
    }

    @Test
    public void updatePreToPostFinal_O() {
        Row oldRow = testRow(oRowType, 25, 2, 25000);
        Row newRow = testRow(oRowType, 25, 2, 25001);
        dmlPreToPostFinal(updateCreator(oID, oldRow, newRow));
    }

    @Test
    public void deletePreToPostFinal_O() {
        dmlPreToPostFinal(deleteCreator(oID, groupRows.get(3)));
    }

    @Test
    public void insertPreToPostFinal_I() {
        Row newRow = testRow(iRowType, 251, 25, 251000);
        dmlPreToPostFinal(insertCreator(iID, newRow));
    }

    @Test
    public void updatePreToPostFinal_I() {
        Row oldRow = testRow(iRowType, 250, 25, 250000);
        Row newRow = testRow(iRowType, 250, 25, 250001);
        dmlPreToPostFinal(updateCreator(iID, oldRow, newRow));
    }

    @Test
    public void deletePreToPostFinal_I() {
        dmlPreToPostFinal(deleteCreator(iID, groupRows.get(4)));
    }
}
