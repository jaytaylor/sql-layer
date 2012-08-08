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
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.expression.std.FieldExpression;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.qp.operator.API.cursor;
import static com.akiban.qp.operator.API.indexScan_Default;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IndexScanNonRootPKIT extends OperatorITBase
{
    @Before
    public void before()
    {
        parent = createTable(
            "schema", "parent",
            "pid int not null",
            "primary key(pid)");
        child = createTable(
            "schema", "child",
            "cid int not null",
            "pid int",
            "primary key(cid)",
            "grouping foreign key (pid) references parent(pid)");
        schema = new Schema(rowDefCache().ais());
        parentRowType = schema.userTableRowType(userTable(parent));
        childRowType = schema.userTableRowType(userTable(child));
        childPKRowType = indexType(child, "cid");
        db = new NewRow[]{
            createNewRow(parent, 1L),
            createNewRow(child, 11L, 1L),
            createNewRow(child, 12L, 1L),
            createNewRow(parent, 2L),
            createNewRow(child, 21L, 2L),
            createNewRow(child, 22L, 2L),
        };
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        use(db);
    }

    // Inspired by bug 1033754

    @Test
    public void testAtChildAtParent()
    {
        long[] childPKs = new long[]{11, 12, 21, 22};
        for (long childPK : childPKs) {
            long parentPK = childPK / 10;
            IndexBound bound = new IndexBound(row(childPKRowType, childPK, parentPK), new SetColumnSelector(0, 1));
            IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, bound, true, bound, true);
            API.Ordering ordering = new API.Ordering();
            ordering.append(Expressions.field(childPKRowType, 0), true);
            ordering.append(Expressions.field(childPKRowType, 1), true);
            Operator plan = indexScan_Default(childPKRowType, range, ordering);
            RowBase[] expected = new RowBase[] {
                row(childRowType, childPK, parentPK),
            };
            compareRows(expected, cursor(plan, queryContext));
        }
    }

    @Test
    public void testAtChildAfterParent()
    {
        long[] childPKs = new long[]{11, 12, 21, 22};
        for (long childPK : childPKs) {
            long parentPK = childPK / 10;
            IndexBound bound = new IndexBound(row(childPKRowType, childPK, parentPK + 1), new SetColumnSelector(0, 1));
            IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, bound, true, bound, true);
            API.Ordering ordering = new API.Ordering();
            ordering.append(Expressions.field(childPKRowType, 0), true);
            ordering.append(Expressions.field(childPKRowType, 1), true);
            Operator plan = indexScan_Default(childPKRowType, range, ordering);
            RowBase[] expected = new RowBase[] {
            };
            compareRows(expected, cursor(plan, queryContext));
        }
    }

    @Test
    public void testAtChildBeforeParent()
    {
        long[] childPKs = new long[]{11, 12, 21, 22};
        for (long childPK : childPKs) {
            long parentPK = childPK / 10;
            IndexBound loBound = new IndexBound(row(childPKRowType, childPK, parentPK - 1), new SetColumnSelector(0, 1));
            IndexBound hiBound = new IndexBound(row(childPKRowType, childPK, parentPK), new SetColumnSelector(0, 1));
            IndexKeyRange range = IndexKeyRange.bounded(childPKRowType, loBound, true, hiBound, true);
            API.Ordering ordering = new API.Ordering();
            ordering.append(Expressions.field(childPKRowType, 0), true);
            ordering.append(Expressions.field(childPKRowType, 1), true);
            Operator plan = indexScan_Default(childPKRowType, range, ordering);
            RowBase[] expected = new RowBase[] {
                row(childRowType, childPK, parentPK),
            };
            compareRows(expected, cursor(plan, queryContext));
        }
    }

    private int parent;
    private int child;
    private RowType parentRowType;
    private RowType childRowType;
    private IndexRowType childPKRowType;
}
