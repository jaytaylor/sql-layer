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

package com.foundationdb.server.test.it.qp;

import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.server.api.dml.SetColumnSelector;
import org.junit.Test;

import java.util.EnumSet;

import static com.foundationdb.qp.operator.API.*;
import static com.foundationdb.server.test.ExpressionGenerators.field;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

// From bug #1081396.
public class GroupSkipScanIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        p = createTable("schema", "p", 
                        "pid INT PRIMARY KEY NOT NULL", 
                        "pn INT");
        createIndex("schema", "p", "p_n", "pn");
        c1 = createTable("schema", "c1",
                         "cid INT PRIMARY KEY NOT NULL", 
                         "pid INT NOT NULL", "GROUPING FOREIGN KEY(pid) REFERENCES p(pid)", 
                         "cn INT");
        createIndex("schema", "c1", "c1_n", "cn");
        c2 = createTable("schema", "c2",
                         "cid INT PRIMARY KEY NOT NULL", 
                         "pid INT NOT NULL", "GROUPING FOREIGN KEY(pid) REFERENCES p(pid)", 
                         "cn INT");
        createIndex("schema", "c2", "c2_n", "cn");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        pRowType = schema.tableRowType(table(p));
        pNIndexRowType = indexType(p, "pn");
        c1RowType = schema.tableRowType(table(c1));
        c1NIndexRowType = indexType(c1, "cn");
        c2RowType = schema.tableRowType(table(c2));
        c2NIndexRowType = indexType(c2, "cn");
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        db = new Row[] {
            row(p, 1L, 1L),
            row(c1, 101L, 1L, 100L),
            row(c1, 102L, 1L, 200L),
            row(c2, 121L, 1L, 120L),
            row(c2, 122L, 1L, 220L),
            row(p, 2L, 2L),
            row(c1, 201L, 2L, 100L),
            row(c1, 202L, 2L, 200L),
            row(c2, 221L, 2L, 120L),
            row(c2, 222L, 2L, 220L),
            row(p, 3L, 1L),
            row(p, 4L, 2L),
            row(p, 5L, 1L),
            row(p, 6L, 2L),
            row(p, 7L, 1L),
            row(p, 8L, 2L),
            row(p, 9L, 1L),
            row(c1, 901L, 9L, 100L),
            row(c1, 902L, 9L, 200L),
            row(c2, 921L, 9L, 120L),
            row(c2, 922L, 9L, 220L),
            row(p, 10L, 2L)
        };
        use(db);
    }

    private static final IntersectOption OUTPUT = IntersectOption.OUTPUT_LEFT;

    private int p, c1, c2;
    private RowType pRowType, c1RowType, c2RowType;
    private IndexRowType pNIndexRowType, c1NIndexRowType, c2NIndexRowType;

    @Test
    public void jumpToEqual()
    {
        Operator plan = jumpToEqual(false);
        Row[] expected = new Row[] {
            row(c2NIndexRowType, 120L, 1L, 121L),
            row(c2NIndexRowType, 120L, 9L, 921L),
        };
        compareRows(expected, cursor(plan, queryContext, queryBindings));
        plan = jumpToEqual(true);
        compareRows(expected, cursor(plan, queryContext, queryBindings));
    }

    private Operator jumpToEqual(boolean skip) 
    {
        Ordering pNOrdering = API.ordering();
        pNOrdering.append(field(pNIndexRowType, 1), true);
        Ordering c1NOrdering = API.ordering();
        c1NOrdering.append(field(c1NIndexRowType, 1), true);
        c1NOrdering.append(field(c1NIndexRowType, 2), true);
        Ordering c2NOrdering = API.ordering();
        c2NOrdering.append(field(c2NIndexRowType, 1), true);
        c2NOrdering.append(field(c1NIndexRowType, 2), true);
        IntersectOption scanType = skip ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN;
        return intersect_Ordered(
                intersect_Ordered(
                        union_Ordered(
                                union_Ordered(
                                        indexScan_Default(
                                                c2NIndexRowType,
                                                nEq(c2NIndexRowType, 120),
                                                c2NOrdering),
                                        indexScan_Default(
                                                c2NIndexRowType,
                                                nEq(c2NIndexRowType, 121),
                                                c2NOrdering),
                                        c2NIndexRowType,
                                        c2NIndexRowType,
                                        2,
                                        2,
                                        ascending(true, true),
                                        true),
                                indexScan_Default(
                                        c2NIndexRowType,
                                        nEq(c2NIndexRowType, 122),
                                        c2NOrdering),
                                c2NIndexRowType,
                                c2NIndexRowType,
                                2,
                                2,
                                ascending(true, true),
                                true),
                        union_Ordered(
                                union_Ordered(
                                        indexScan_Default(
                                                pNIndexRowType,
                                                nEq(pNIndexRowType, 0),
                                                pNOrdering),
                                        indexScan_Default(
                                                pNIndexRowType,
                                                nEq(pNIndexRowType, 1),
                                                pNOrdering),
                                        pNIndexRowType,
                                        pNIndexRowType,
                                        1,
                                        1,
                                        ascending(true),
                                        false),
                                indexScan_Default(
                                        pNIndexRowType,
                                        nEq(pNIndexRowType, 3),
                                        pNOrdering),
                                pNIndexRowType,
                                pNIndexRowType,
                                1,
                                1,
                                ascending(true),
                                false),
                        c2NIndexRowType,
                        pNIndexRowType,
                        2,
                        1,
                        ascending(true),
                        JoinType.INNER_JOIN,
                        EnumSet.of(OUTPUT, scanType),
                        null,
                        true),
                union_Ordered(
                        union_Ordered(
                                indexScan_Default(
                                        c1NIndexRowType,
                                        nEq(c1NIndexRowType, 100),
                                        c1NOrdering),
                                indexScan_Default(
                                        c1NIndexRowType,
                                        nEq(c1NIndexRowType, 101),
                                        c1NOrdering),
                                c1NIndexRowType,
                                c1NIndexRowType,
                                2,
                                2,
                                ascending(true, true),
                                false),
                        indexScan_Default(
                                c1NIndexRowType,
                                nEq(c1NIndexRowType, 102),
                                c1NOrdering),
                        c1NIndexRowType,
                        c1NIndexRowType,
                        2,
                        2,
                        ascending(true, true),
                        false),
                c2NIndexRowType,
                c1NIndexRowType,
                2,
                2,
                ascending(true),
                JoinType.INNER_JOIN,
                EnumSet.of(OUTPUT, scanType),
                null,
                true);
    }

    private IndexKeyRange nEq(IndexRowType nIndexRowType, long n)
    {
        IndexBound bound = new IndexBound(row(nIndexRowType, n), new SetColumnSelector(0));
        return IndexKeyRange.bounded(nIndexRowType, bound, true, bound, true);
    }

    private boolean[] ascending(boolean... ascending)
    {
        return ascending;
    }

}
