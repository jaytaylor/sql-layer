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
            createNewRow(t, 1001L, 1L, 11L, 112L),
            createNewRow(t, 1002L, 1L, 12L, 121L),
            createNewRow(t, 1003L, 1L, 12L, 122L),
            createNewRow(t, 1004L, 2L, 21L, 211L),
            createNewRow(t, 1005L, 2L, 21L, 212L),
            createNewRow(t, 1006L, 2L, 22L, 221L),
            createNewRow(t, 1007L, 2L, 22L, 222L),
            // With nulls
            createNewRow(t, 2000L, 3L, 4L, 5L),
            createNewRow(t, 2001L, 3L, 4L, null),
            createNewRow(t, 2002L, 3L, null, 5L),
            createNewRow(t, 2003L, 3L, null, null),
            createNewRow(t, 2004L, null, 4L, 5L),
            createNewRow(t, 2005L, null, 4L, null),
            createNewRow(t, 2006L, null, null, 5L),
            createNewRow(t, 2007L, null, null, null),
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

    // Test name: test_AB_CD_O
    // A: Lo Inclusive/Exclusive/Unbounded
    // B: Lo Bound is present/missing
    // C: Hi Inclusive/Exclusive/Unbounded
    // D: Hi Bound is present/missing
    // O: Ordering: A/D for each ordering field

    @Test
    public void test_IP_IP_AAA()
    {
        {
            Operator plan = indexScan_Default(idxRowType,
                                              range(true, 1, UNSPECIFIED, UNSPECIFIED,
                                                    true, 2, UNSPECIFIED, UNSPECIFIED),
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

    @Test
    public void test_IP_IP_AAD()
    {
        {
            Operator plan = indexScan_Default(idxRowType,
                                              range(true, 1, UNSPECIFIED, UNSPECIFIED,
                                                    true, 2, UNSPECIFIED, UNSPECIFIED),
                                              ordering(A, ASC, B, ASC, C, DESC));
            RowBase[] expected = new RowBase[]{
                row(idxRowType, 1L, 11L, 112L, 1001L),
                row(idxRowType, 1L, 11L, 111L, 1000L),
                row(idxRowType, 1L, 12L, 122L, 1003L),
                row(idxRowType, 1L, 12L, 121L, 1002L),
                row(idxRowType, 2L, 21L, 212L, 1005L),
                row(idxRowType, 2L, 21L, 211L, 1004L),
                row(idxRowType, 2L, 22L, 222L, 1007L),
                row(idxRowType, 2L, 22L, 221L, 1006L),
            };
            compareRows(expected, cursor(plan, adapter));
        }
        {
            Operator plan = indexScan_Default(idxRowType,
                                              range(true, 1, 12, UNSPECIFIED,
                                                    true, 1, 12, UNSPECIFIED),
                                              ordering(A, ASC, B, ASC, C, DESC));
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
                                              ordering(A, ASC, B, ASC, C, DESC));
            RowBase[] expected = new RowBase[]{
                row(idxRowType, 1L, 12L, 122L, 1003L),
                row(idxRowType, 1L, 12L, 121L, 1002L),
            };
            compareRows(expected, cursor(plan, adapter));
        }
    }

    // For use by this class

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

    private API.Ordering ordering(Object ... ord) // alternating column positions and asc/desc
    {
        API.Ordering ordering = API.ordering();
        int i = 0;
        while (i < ord.length) {
            int column = (Integer) ord[i++];
            boolean asc = (Boolean) ord[i++];
            ordering.append(new FieldExpression(idxRowType, column), asc);
        }
        return ordering;
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
