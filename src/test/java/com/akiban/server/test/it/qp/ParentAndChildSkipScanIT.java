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
import static com.akiban.server.expression.std.Expressions.field;

public class ParentAndChildSkipScanIT extends OperatorITBase
{
    @Before
    public void before()
    {
        parent = createTable(
            "schema", "parent",
            "pid int not null",
            "x int",
            "primary key(pid)");
        createIndex("schema", "parent", "idx_x", "x");
        child = createTable(
            "schema", "child",
            "cid int not null primary key",
            "pid int",
            "y int",
            "grouping foreign key (pid) references parent(pid)");
        createIndex("schema", "child", "y", "y");
        schema = new Schema(ais());
        parentRowType = schema.userTableRowType(userTable(parent));
        childRowType = schema.userTableRowType(userTable(child));
        parentPidIndexRowType = indexType(parent, "pid");
        parentXIndexRowType = indexType(parent, "x");
        childYIndexRowType = indexType(child, "y");
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        db = new NewRow[]{
            createNewRow(parent, 60L, 1L),
            createNewRow(child, 7000L, 70L, 2L),
            createNewRow(parent, 80L, 1L),
            createNewRow(child, 8000L, 80L, 2L),
            createNewRow(child, 9000L, 90L, 2L),
        };
        use(db);
    }

    private int parent;
    private int child;
    private RowType parentRowType;
    private RowType childRowType;
    private IndexRowType parentPidIndexRowType;
    private IndexRowType parentXIndexRowType;
    private IndexRowType childYIndexRowType;

    @Test
    public void test8x()
    {
        RowBase[] expected = new RowBase[] {
            row(childRowType, 2L, 80L, 8000L)
        };
        compareRows(expected, cursor(intersectPxUnionCy(1, 2, 3, JoinType.INNER_JOIN, true, false), queryContext));
        compareRows(expected, cursor(intersectPxUnionCy(1, 2, 3, JoinType.INNER_JOIN, true, true), queryContext));
        compareRows(expected, cursor(intersectPxUnionCy(1, 2, 3, JoinType.INNER_JOIN, false, false), queryContext));
        compareRows(expected, cursor(intersectPxUnionCy(1, 2, 3, JoinType.INNER_JOIN, false, true), queryContext));
    }

    private Operator intersectPxUnionCy(int x, int y1, int y2, JoinType joinType, boolean ascending, boolean skipScan)
    {
        Ordering xOrdering = ordering(field(parentXIndexRowType, 1), ascending);
        Ordering yOrdering = ordering(field(childYIndexRowType, 1), ascending,
                                      field(childYIndexRowType, 2), ascending);
        return
            intersect_Ordered(
                indexScan_Default(
                    parentXIndexRowType,
                    parentXEq(x),
                    xOrdering),
                union_Ordered(
                    indexScan_Default(
                        childYIndexRowType,
                        childYEq(y1),
                        yOrdering),
                    indexScan_Default(
                        childYIndexRowType,
                        childYEq(y2),
                        yOrdering),
                    childYIndexRowType,
                    childYIndexRowType,
                    2,
                    2,
                    new boolean[]{ascending, ascending}),
                parentXIndexRowType,
                childYIndexRowType,
                1,
                2,
                ascending(ascending),
                joinType,
                EnumSet.of(skipScan ? IntersectOption.SKIP_SCAN : IntersectOption.SEQUENTIAL_SCAN,
                           IntersectOption.OUTPUT_RIGHT));
    }

    private IndexKeyRange parentXEq(long x)
    {
        IndexBound xBound = new IndexBound(row(parentXIndexRowType, x), new SetColumnSelector(0));
        return IndexKeyRange.bounded(parentXIndexRowType, xBound, true, xBound, true);
    }

    private IndexKeyRange childYEq(long y)
    {
        IndexBound yBound = new IndexBound(row(childYIndexRowType, y), new SetColumnSelector(0));
        return IndexKeyRange.bounded(childYIndexRowType, yBound, true, yBound, true);
    }

    private Ordering ordering(Object... objects)
    {
        Ordering ordering = API.ordering();
        int i = 0;
        while (i < objects.length) {
            Expression expression = (Expression) objects[i++];
            Boolean ascending = (Boolean) objects[i++];
            ordering.append(expression, ascending);
        }
        return ordering;
    }

    private boolean[] ascending(boolean ... ascending)
    {
        return ascending;
    }
}
