/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.it.qp;

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.persistitadapter.OperatorStore;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.std.FieldExpression;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.Store;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
 * This test covers bounded index scans with combinations of the following variations:
 * - ascending/descending/mixed order
 * - inclusive/exclusive/semi-bounded
 * - bound is present/missing
 */

public class IndexScanBoundedIT extends OperatorITBase
{
    @Before
    public void before()
    {
        t = createTable(
            "schema", "t",
            "id int not null key",
            "a int",
            "b int",
            "c int",
            "index(a, b, c, id)");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        idxRowType = indexType(t, "a", "b", "c", "id");
        db = new NewRow[]{
            // No nulls
            createNewRow(t, 1000L, 1L, 11L, 111L),
            createNewRow(t, 1001L, 1L, 11L, 115L),
            createNewRow(t, 1002L, 1L, 15L, 151L),
            createNewRow(t, 1003L, 1L, 15L, 155L),
            createNewRow(t, 1004L, 5L, 51L, 511L),
            createNewRow(t, 1005L, 5L, 51L, 515L),
            createNewRow(t, 1006L, 5L, 55L, 551L),
            createNewRow(t, 1007L, 5L, 55L, 555L),
        };
        Store plainStore = store();
        final PersistitStore persistitStore;
        if (plainStore instanceof OperatorStore) {
            OperatorStore operatorStore = (OperatorStore) plainStore;
            persistitStore = operatorStore.getPersistitStore();
        } else {
            persistitStore = (PersistitStore) plainStore;
        }
        adapter = new PersistitAdapter(schema, persistitStore, null, session(), configService());
        use(db);
    }

    // Test name: test_AB_CD
    // A: Lo Inclusive/Exclusive/Unbounded
    // B: Lo Bound is present/missing (relevant only if lo is not unbounded)
    // C: Hi Inclusive/Exclusive/Unbounded
    // D: Hi Bound is present/missing (relevant only if hi is not unbounded)
    //
    // AB/CD combinations are not tested exhaustively because processing at
    // start and end of scan are independent.
    // However, there is some testing around empty and single-key ranges.

    @Test
    public void test_U_IP()
    {
        // AAA
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 51, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 55, 555),
             ordering(ASC, ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        // AA
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 51, UNSPECIFIED),
             ordering(ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 55, 555),
             ordering(ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        // A
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 51, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 55, 555),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        // AAD
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1001, 1000, 1003, 1002);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1001, 1000, 1003, 1002);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 1, 15, 151),
             ordering(ASC, ASC, DESC),
             1001, 1000, 1002);
        // AA
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC),
             1000, 1001, 1002, 1003);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, ASC),
             1000, 1001, 1002, 1003);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 1, 15, 151),
             ordering(ASC, ASC),
             1000, 1001, 1002);
        // A
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 1, 15, 151),
             ordering(ASC),
             1000, 1001, 1002);
        // ADA
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, ASC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 51, UNSPECIFIED),
             ordering(ASC, DESC, ASC),
             1002, 1003, 1000, 1001, 1004, 1005);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 55, 551),
             ordering(ASC, DESC, ASC),
             1002, 1003, 1000, 1001, 1006, 1004, 1005);
        // AD
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 51, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1004, 1005);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 55, 551),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        // A
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 51, UNSPECIFIED),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 55, 551),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006);
        // ADD
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1003, 1002, 1001, 1000, 1007, 1006, 1005, 1004);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 51, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1003, 1002, 1001, 1000, 1005, 1004);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 55, 551),
             ordering(ASC, DESC, DESC),
             1003, 1002, 1001, 1000, 1006, 1005, 1004);
        // AD
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        dump(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 51, UNSPECIFIED),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 55, 551),
             ordering(ASC, DESC),
             1002, 1003, 1000, 1001, 1006);
        // DAA
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1000, 1001, 1002, 1003);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 1, 11, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1000, 1001);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 1, 11, 111),
             ordering(ASC, DESC, DESC),
             1000);
        // DAD
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1005, 1004, 1007, 1006, 1001, 1000, 1003, 1002);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 51, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1005, 1004, 1001, 1000, 1003, 1002);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 51, 515),
             ordering(DESC, ASC, DESC),
             1005, 1004, 1001, 1000, 1003, 1002);
        // DDA
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1006, 1007, 1004, 1005, 1002, 1003, 1000, 1001);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 51, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1004, 1005, 1002, 1003, 1000, 1001);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 51, 515),
             ordering(DESC, DESC, ASC),
             1004, 1005, 1002, 1003, 1000, 1001);
        // DDD
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 51, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1005, 1004, 1003, 1002, 1001, 1000);
        test(range(EXCLUSIVE, UNSPECIFIED, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, 51, 515),
             ordering(DESC, DESC, DESC),
             1005, 1004, 1003, 1002, 1001, 1000);
    }

    @Test
    public void test_IP_IP()
    {
        // AAA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, ASC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(ASC, ASC, ASC),
             1002, 1003);
        // AAD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1001, 1000, 1003, 1002, 1005, 1004, 1007, 1006);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, ASC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(ASC, ASC, DESC),
             1003, 1002);
        // ADA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, ASC),
             1002, 1003, 1000, 1001, 1006, 1007, 1004, 1005);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, DESC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(ASC, DESC, ASC),
             1002, 1003);
        // ADD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1003, 1002, 1001, 1000, 1007, 1006, 1005, 1004);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(ASC, DESC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(ASC, DESC, DESC),
             1003, 1002);
        // DAA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1004, 1005, 1006, 1007, 1000, 1001, 1002, 1003);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, ASC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(DESC, ASC, ASC),
             1002, 1003);
        // DAD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1005, 1004, 1007, 1006, 1001, 1000, 1003, 1002);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, ASC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(DESC, ASC, DESC),
             1003, 1002);
        // DDA
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1006, 1007, 1004, 1005, 1002, 1003, 1000, 1001);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, DESC, ASC),
             1002, 1003);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(DESC, DESC, ASC),
             1002, 1003);
        // DDD
        test(range(INCLUSIVE, 1, UNSPECIFIED, UNSPECIFIED,
                   INCLUSIVE, 5, UNSPECIFIED, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
        test(range(INCLUSIVE, 1, 15, UNSPECIFIED,
                   INCLUSIVE, 1, 15, UNSPECIFIED),
             ordering(DESC, DESC, DESC),
             1003, 1002);
        test(range(INCLUSIVE, 1, 15, 151,
                   INCLUSIVE, 1, 15, 155),
             ordering(DESC, DESC, DESC),
             1003, 1002);
    }

/*
    @Test
    public void test_IP_IP_DDD()
    {
        {
            Operator plan = indexScan_Default(idxRowType,
                                              range(true, 1, UNSPECIFIED, UNSPECIFIED,
                                                    true, 2, UNSPECIFIED, UNSPECIFIED),
                                              ordering(A, DESC, B, DESC, C, DESC));
            RowBase[] expected = new RowBase[]{
                row(idxRowType, 2L, 22L, 222L, 1007L),
                row(idxRowType, 2L, 22L, 221L, 1006L),
                row(idxRowType, 2L, 21L, 212L, 1005L),
                row(idxRowType, 2L, 21L, 211L, 1004L),
                row(idxRowType, 1L, 12L, 122L, 1003L),
                row(idxRowType, 1L, 12L, 121L, 1002L),
                row(idxRowType, 1L, 11L, 112L, 1001L),
                row(idxRowType, 1L, 11L, 111L, 1000L),
            };
            compareRows(expected, cursor(plan, adapter));
        }
        {
            Operator plan = indexScan_Default(idxRowType,
                                              range(true, 1, 12, UNSPECIFIED,
                                                    true, 1, 12, UNSPECIFIED),
                                              ordering(A, DESC, B, DESC, C, DESC));
            RowBase[] expected = new RowBase[]{
                row(idxRowType, 1L, 12L, 122L, 1003L),
                row(idxRowType, 1L, 12L, 121L, 1002L),
            };
            compareRows(expected, cursor(plan, adapter));
        }
        {
            Operator plan = indexScan_Default(idxRowType,
                                              range(true, 1, 12, 121,
                                                    true, 1, 12, 122),
                                              ordering(A, DESC, B, DESC, C, DESC));
            RowBase[] expected = new RowBase[]{
                row(idxRowType, 1L, 12L, 122L, 1003L),
                row(idxRowType, 1L, 12L, 121L, 1002L),
            };
            compareRows(expected, cursor(plan, adapter));
        }
    }

    @Test
    public void test_IP_IM_AAA()
    {
        {
            Operator plan = indexScan_Default(idxRowType,
                                              range(true, 1, UNSPECIFIED, UNSPECIFIED,
                                                    true, 4, UNSPECIFIED, UNSPECIFIED),
                                              ordering(A, ASC, B, ASC, C, ASC));
            RowBase[] expected = new RowBase[]{
                row(idxRowType, 1L, 11L, 111L, 1000L),
                row(idxRowType, 1L, 11L, 112L, 1001L),
                row(idxRowType, 1L, 12L, 121L, 1002L),
                row(idxRowType, 1L, 12L, 122L, 1003L),
                row(idxRowType, 2L, 21L, 211L, 1004L),
                row(idxRowType, 2L, 21L, 212L, 1005L),
                row(idxRowType, 2L, 22L, 221L, 1006L),
                row(idxRowType, 2L, 22L, 222L, 1007L),
            };
            compareRows(expected, cursor(plan, adapter));
        }
        {
            Operator plan = indexScan_Default(idxRowType,
                                              range(true, 1, 12, UNSPECIFIED,
                                                    true, 1, 12, UNSPECIFIED),
                                              ordering(A, ASC, B, ASC, C, ASC));
            RowBase[] expected = new RowBase[]{
                row(idxRowType, 1L, 12L, 121L, 1002L),
                row(idxRowType, 1L, 12L, 122L, 1003L),
            };
            compareRows(expected, cursor(plan, adapter));
        }
        {
            Operator plan = indexScan_Default(idxRowType,
                                              range(true, 1, 12, 121,
                                                    true, 1, 12, 122),
                                              ordering(A, ASC, B, ASC, C, ASC));
            RowBase[] expected = new RowBase[]{
                row(idxRowType, 1L, 12L, 121L, 1002L),
                row(idxRowType, 1L, 12L, 122L, 1003L),
            };
            compareRows(expected, cursor(plan, adapter));
        }
    }
*/

        // For use by this class

    private void test(IndexKeyRange keyRange, API.Ordering ordering, int ... expectedIds)
    {
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        RowBase[] expected = new RowBase[expectedIds.length];
        for (int i = 0; i < expectedIds.length; i++) {
            int id = expectedIds[i];
            expected[i] = dbRow(id);
        }
        compareRows(expected, cursor(plan, adapter));
    }

    private void dump(IndexKeyRange keyRange, API.Ordering ordering, int ... expectedIds)
    {
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        dumpToAssertion(plan);
    }

    private IndexKeyRange range(boolean loInclusive, Integer aLo, Integer bLo, Integer cLo,
                                boolean hiInclusive, Integer aHi, Integer bHi, Integer cHi)
    {
        IndexBound lo;
        if (aLo == UNSPECIFIED) {
            lo = null;
        } else if (bLo == UNSPECIFIED) {
            lo = new IndexBound(row(idxRowType, aLo), new SetColumnSelector(0));
        } else if (cLo == UNSPECIFIED) {
            lo = new IndexBound(row(idxRowType, aLo, bLo), new SetColumnSelector(0, 1));
        } else {
            lo = new IndexBound(row(idxRowType, aLo, bLo, cLo), new SetColumnSelector(0, 1, 2));
        }
        IndexBound hi;
        if (aHi == UNSPECIFIED) {
            hi = null;
        } else if (bHi == UNSPECIFIED) {
            hi = new IndexBound(row(idxRowType, aHi), new SetColumnSelector(0));
        } else if (cHi == UNSPECIFIED) {
            hi = new IndexBound(row(idxRowType, aHi, bHi), new SetColumnSelector(0, 1));
        } else {
            hi = new IndexBound(row(idxRowType, aHi, bHi, cHi), new SetColumnSelector(0, 1, 2));
        }
        return new IndexKeyRange(idxRowType, lo, loInclusive, hi, hiInclusive);
    }

    private API.Ordering ordering(boolean... directions)
    {
        assertTrue(directions.length >= 1 && directions.length <= 3);
        API.Ordering ordering = API.ordering();
        if (directions.length >= 1) {
            ordering.append(new FieldExpression(idxRowType, A), directions[0]);
        }
        if (directions.length >= 2) {
            ordering.append(new FieldExpression(idxRowType, B), directions[1]);
        }
        if (directions.length >= 3) {
            ordering.append(new FieldExpression(idxRowType, C), directions[2]);
        }
        return ordering;
    }

    private RowBase dbRow(long id)
    {
        for (NewRow newRow : db) {
            if (newRow.get(0).equals(id)) {
                return row(idxRowType, newRow.get(1), newRow.get(2), newRow.get(3), newRow.get(0));
            }
        }
        fail();
        return null;
    }

    // Positions of fields within the index row
    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;
    private static final int ID = 3;
    private static final boolean ASC = true;
    private static final boolean DESC = false;
    private static final boolean EXCLUSIVE = false;
    private static final boolean INCLUSIVE = true;
    private static final Integer UNSPECIFIED = new Integer(Integer.MIN_VALUE);

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
}
