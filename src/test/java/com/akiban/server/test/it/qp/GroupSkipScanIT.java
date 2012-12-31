/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.it.qp;

import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.Expression;
import org.junit.Before;
import org.junit.Test;

import java.util.EnumSet;

import static com.akiban.qp.operator.API.*;
import static com.akiban.server.test.ExpressionGenerators.field;
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
        schema = new Schema(ais());
        pRowType = schema.userTableRowType(userTable(p));
        pNIndexRowType = indexType(p, "pn");
        c1RowType = schema.userTableRowType(userTable(c1));
        c1NIndexRowType = indexType(c1, "cn");
        c2RowType = schema.userTableRowType(userTable(c2));
        c2NIndexRowType = indexType(c2, "cn");
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        db = new NewRow[] {
            createNewRow(p, 1L, 1L),
            createNewRow(c1, 101L, 1L, 100L),
            createNewRow(c1, 102L, 1L, 200L),
            createNewRow(c2, 121L, 1L, 120L),
            createNewRow(c2, 122L, 1L, 220L),
            createNewRow(p, 2L, 2L),
            createNewRow(c1, 201L, 2L, 100L),
            createNewRow(c1, 202L, 2L, 200L),
            createNewRow(c2, 221L, 2L, 120L),
            createNewRow(c2, 222L, 2L, 220L),
            createNewRow(p, 3L, 1L),
            createNewRow(p, 4L, 2L),
            createNewRow(p, 5L, 1L),
            createNewRow(p, 6L, 2L),
            createNewRow(p, 7L, 1L),
            createNewRow(p, 8L, 2L),
            createNewRow(p, 9L, 1L),
            createNewRow(c1, 901L, 9L, 100L),
            createNewRow(c1, 902L, 9L, 200L),
            createNewRow(c2, 921L, 9L, 120L),
            createNewRow(c2, 922L, 9L, 220L),
            createNewRow(p, 10L, 2L)
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
        RowBase[] expected = new RowBase[] {
            row(c2NIndexRowType, 120L, 1L, 121L),
            row(c2NIndexRowType, 120L, 9L, 921L),
        };
        compareRows(expected, cursor(plan, queryContext));
        plan = jumpToEqual(true);
        compareRows(expected, cursor(plan, queryContext));
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
                EnumSet.of(OUTPUT, scanType)),
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
            EnumSet.of(OUTPUT, scanType));
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
