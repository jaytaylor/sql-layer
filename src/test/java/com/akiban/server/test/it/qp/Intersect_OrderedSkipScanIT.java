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
import static junit.framework.Assert.fail;

public class Intersect_OrderedSkipScanIT extends OperatorITBase
{
    @Before
    public void before()
    {
        parent = createTable(
            "schema", "parent",
            "pid int not null primary key",
            "x int",
            "y int");
        createIndex("schema", "parent", "x", "x");
        createIndex("schema", "parent", "y", "y");
        child = createTable(
            "schema", "child",
            "cid int not null primary key",
            "pid int",
            "z int",
            "grouping foreign key (pid) references parent(pid)");
        createIndex("schema", "child", "z", "z");
        schema = new Schema(rowDefCache().ais());
        parentRowType = schema.userTableRowType(userTable(parent));
        childRowType = schema.userTableRowType(userTable(child));
        parentPidIndexRowType = indexType(parent, "pid");
        parentXIndexRowType = indexType(parent, "x");
        parentYIndexRowType = indexType(parent, "y");
        childZIndexRowType = indexType(child, "z");
        coi = groupTable(parent);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        db = new NewRow[]{
            createNewRow(parent, 6000L, -1L, 66L),
            createNewRow(parent, 6001L, -1L, 66L),
            createNewRow(parent, 6002L, 66L, 66L),
            createNewRow(parent, 6003L, 66L, -1L),
            createNewRow(parent, 6004L, 66L, -1L),
            createNewRow(parent, 6005L, -1L, 66L),
            createNewRow(parent, 6006L, -1L, 66L),
            createNewRow(parent, 6007L, -1L, 66L),
        };
        use(db);
    }

    private int parent;
    private int child;
    private RowType parentRowType;
    private RowType childRowType;
    private IndexRowType parentPidIndexRowType;
    private IndexRowType parentXIndexRowType;
    private IndexRowType parentYIndexRowType;
    private IndexRowType childZIndexRowType;

    @Test
    public void test6x()
    {
        Operator plan = intersectPxPy(66, true);
        RowBase[] expected = new RowBase[]{
            row(parentXIndexRowType, 66L, 6002L),
            row(parentXIndexRowType, 66L, 6003L),
        };
        dump(cursor(plan, queryContext));
    }

    private Operator intersectPxPy(int key, boolean ascending)
    {
        Operator plan =
            intersect_Ordered(
                indexScan_Default(
                    parentXIndexRowType,
                    parentXEq(key),
                    ordering(field(parentXIndexRowType, 1), ascending)),
                indexScan_Default(
                    parentYIndexRowType,
                    parentYEq(key),
                    ordering(field(parentYIndexRowType, 1), ascending)),
                parentXIndexRowType,
                parentYIndexRowType,
                1,
                1,
                ascending(ascending),
                JoinType.INNER_JOIN,
                EnumSet.of(IntersectOption.OUTPUT_LEFT));
        return plan;
    }

    private Operator intersectPxCz(int key, JoinType joinType, boolean ascending)
    {
        Operator plan =
            intersect_Ordered(
                indexScan_Default(
                    parentXIndexRowType,
                    parentXEq(key),
                    ordering(field(parentXIndexRowType, 1), ascending)),
                indexScan_Default(
                    childZIndexRowType,
                    childZEq(key),
                    ordering(field(childZIndexRowType, 1), ascending,
                             field(childZIndexRowType, 2), ascending)),
                    parentXIndexRowType,
                    childZIndexRowType,
                    1,
                    2,
                    ascending(ascending),
                    joinType,
                    EnumSet.of(IntersectOption.OUTPUT_RIGHT));
        return plan;
    }

    private IndexKeyRange parentXEq(long x)
    {
        IndexBound xBound = new IndexBound(row(parentXIndexRowType, x), new SetColumnSelector(0));
        return IndexKeyRange.bounded(parentXIndexRowType, xBound, true, xBound, true);
    }

    private IndexKeyRange parentYEq(long y)
    {
        IndexBound yBound = new IndexBound(row(parentYIndexRowType, y), new SetColumnSelector(0));
        return IndexKeyRange.bounded(parentYIndexRowType, yBound, true, yBound, true);
    }

    private IndexKeyRange childZEq(long z)
    {
        IndexBound zBound = new IndexBound(row(childZIndexRowType, z), new SetColumnSelector(0));
        return IndexKeyRange.bounded(childZIndexRowType, zBound, true, zBound, true);
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
