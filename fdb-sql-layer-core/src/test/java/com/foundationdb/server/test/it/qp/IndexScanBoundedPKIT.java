/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

package com.foundationdb.server.test.it.qp;

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.expression.RowBasedUnboundExpressions;
import com.foundationdb.qp.expression.UnboundExpressions;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.types.texpressions.TNullExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedLiteral;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSources;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.foundationdb.qp.operator.API.cursor;
import static com.foundationdb.qp.operator.API.indexScan_Default;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/*
 * This test covers bounded index scans with combinations of the following variations:
 * - ascending/descending/mixed order
 * - inclusive/exclusive/semi-bounded
 * - bound is present/missing
 */

public class IndexScanBoundedPKIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null primary key",
            "a int",
            "b int",
            "c int");
        createIndex("schema", "t", "a", "a", "b", "c", "id");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        tRowType = schema.tableRowType(table(t));
        pkRowType = indexType(t, "id");
        idxRowType = indexType(t, "a", "b", "c", "id");
        db = new Row[]{
            // No nulls
            row(t, 1000L, 1L, 11L, 111L),
            row(t, 1001L, 1L, 11L, 115L),
            row(t, 1002L, 1L, 15L, 151L),
            row(t, 1003L, 1L, 15L, 155L),
            row(t, 1004L, 5L, 51L, 511L),
            row(t, 1005L, 5L, 51L, 515L),
            row(t, 1006L, 5L, 55L, 551L),
            row(t, 1007L, 5L, 55L, 555L),
        };
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        use(db);
    }

    // Test name: test_AB_CD
    // A: Lo Inclusive/Exclusive
    // B: Lo Bound is present/missing
    // C: Hi Inclusive/Exclusive
    // D: Hi Bound is present/missing
    // AB/CD combinations are not tested exhaustively because processing at
    // start and end of scan are independent.

    @Test
    public void test_IP_IP_A()
    {
        // A
        test(pkRange(INCLUSIVE, 1, INCLUSIVE, 5),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
    }

    @Test
    public void test_IP_IP_D()
    {
        // D
        test(pkRange(INCLUSIVE, 1,INCLUSIVE, 5),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
    }

    
    @Test
    public void test_IM_IM_A()
    {
        // A
        test(pkRange(INCLUSIVE, 0, INCLUSIVE, 6),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
    }
    @Test
    public void test_IM_IM_D() 
    {
        // D
        test(pkRange(INCLUSIVE, 0, INCLUSIVE, 6),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
    }
    @Test
    public void test_EP_EP_A()
    {
        // A
        test(pkRange(EXCLUSIVE, 1, EXCLUSIVE, 5),
             ordering(ASC));
    }
    @Test
    public void test_EP_EP_D()
    {
        // D
        test(pkRange(EXCLUSIVE, 1,EXCLUSIVE, 5),
             ordering(DESC));
    }
    
    @Test
    public void test_EM_EM_A()
    {
        // A
        test(pkRange(EXCLUSIVE, 0, EXCLUSIVE, 6),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
    }
    @Test
    public void test_EM_EM_D()
    {
        // D
        test(pkRange(EXCLUSIVE, 0, EXCLUSIVE, 6),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
    }


    // Test half-bounded ranges

    @Test
    public void testBoundedLeftInclusive_A()
    {
        // A
        test(pkRange(INCLUSIVE, 1, EXCLUSIVE, null),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
    }
    @Test
    public void testBoundedLeftInclusive_D()
    {
        // D
        test(pkRange(INCLUSIVE, 1, EXCLUSIVE, null),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
    }

    @Test
    public void testBoundedLeftExclusive_A()
    {
        // A
        test(pkRange(EXCLUSIVE, 3, EXCLUSIVE, null),
             ordering(ASC),
             1004, 1005, 1006, 1007);
    }
    @Test
    public void testBoundedLeftExclusive_D()
    {
        // D
        test(pkRange(EXCLUSIVE, 3, EXCLUSIVE, null),
             ordering(DESC),
             1007, 1006, 1005, 1004);
    }

    @Test
    public void testBoundedRightInclusive_A()
    {
        // A
        test(pkRange(EXCLUSIVE, null, INCLUSIVE, 5),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
    }
    @Test
    public void testBoundedRightInclusive_D()
    {
        // D
        test(pkRange(EXCLUSIVE, null, INCLUSIVE, 5),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
    }

    @Test
    public void testBoundedRightExclusive_A()
    {
        // A
        test(pkRange(EXCLUSIVE, null,EXCLUSIVE, 8),
             ordering(ASC),
             1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007);
    }
    @Test
    public void testBoundedRightExclusive_D()
    {
        // D
        test(pkRange(EXCLUSIVE, null, EXCLUSIVE, 8),
             ordering(DESC),
             1007, 1006, 1005, 1004, 1003, 1002, 1001, 1000);
    }

    // case 2: > null <= null
    @Test(expected=IllegalArgumentException.class)
    public void leftExclusiveNullRightInclusiveNull() {
        testEmptySet(pkRange(EXCLUSIVE, null, INCLUSIVE, null),
             ordering(ASC));
    }

    // case 6: > non-null, <= null
    @Test(expected=IllegalArgumentException.class)
    public void leftExclusiveNonNullRightInclusiveNull() {
        testEmptySet(pkRange(EXCLUSIVE, 1000, INCLUSIVE, null),
             ordering(ASC));
    }

    // case 10: >= null, <= null
    @Test
    public void leftInclusiveNullRightInclusiveNull() {
        Row row = row(t, 2000L, null, 11L, 111L);
        writeRows(row);
        db = Arrays.copyOf(db, db.length + 1);
        db[db.length -1] = row;
        test(pkRange(INCLUSIVE, null, INCLUSIVE, null),
             ordering(ASC),
             2000);
    }

    // case 14: >= non-null, <= null
    @Test(expected=IllegalArgumentException.class)
    public void leftInclusiveNonNullRightInclusiveNull() {
        testEmptySet(pkRange(INCLUSIVE, 1000, INCLUSIVE, null),
             ordering(ASC));
    }

    // For use by this class

    private void test(IndexKeyRange keyRange, API.Ordering ordering, int ... expectedIds)
    {
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        Row[] expected = new Row[expectedIds.length];
        for (int i = 0; i < expectedIds.length; i++) {
            int id = expectedIds[i];
            expected[i] = dbRow(id);
        }
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }
    
    private void testEmptySet (IndexKeyRange keyRange, API.Ordering ordering) 
    {
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        Row[] expected = new Row[0];
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    private void dump(IndexKeyRange keyRange, API.Ordering ordering, int ... expectedIds)
    {
        Operator plan = indexScan_Default(idxRowType, keyRange, ordering);
        dumpToAssertion(plan);
    }

    private UnboundExpressions pkExprRow (Integer a) {
        List<TPreparedExpression> pExprs = new ArrayList<>(1);
        if (a == null) {
            pExprs.add(new TNullExpression(pkRowType.typeAt(0)));
        } else {
            pExprs.add(new TPreparedLiteral(new Value(pkRowType.typeAt(0), a)));
        }
        return  new RowBasedUnboundExpressions(pkRowType, pExprs);
    }
    
    private IndexKeyRange pkRange(boolean loInclusive, Integer aLo, 
                                  boolean hiInclusive, Integer aHi) 
    {
        IndexBound lo = new IndexBound(pkExprRow(aLo), new SetColumnSelector(0));
        IndexBound hi = new IndexBound(pkExprRow(aHi), new SetColumnSelector(0));
        return IndexKeyRange.bounded(pkRowType, lo, loInclusive, hi, hiInclusive);
    }
    
    private API.Ordering ordering(boolean direction) {
        API.Ordering ordering = API.ordering();
        ordering.append(field(pkRowType, 0), direction);
        return ordering;
    }

    private Row dbRow(long id)
    {
        for (Row newRow : db) {
            if (ValueSources.getLong(newRow.value(0)) == id) {
                return row(idxRowType,
                           ValueSources.toObject(newRow.value(1)),
                           ValueSources.toObject(newRow.value(2)),
                           ValueSources.toObject(newRow.value(3)),
                           ValueSources.toObject(newRow.value(0)));
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
    private static final Integer UNSPECIFIED = new Integer(Integer.MIN_VALUE); // Relying on == comparisons

    private int t;
    private RowType tRowType;
    private IndexRowType idxRowType;
    private IndexRowType pkRowType;
}
