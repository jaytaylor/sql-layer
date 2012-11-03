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

import com.akiban.ais.model.Group;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.test.ExpressionGenerators;
import org.junit.Before;
import org.junit.Test;

import static com.akiban.qp.operator.API.*;
import static com.akiban.server.test.ExpressionGenerators.field;

public class UnionAll_DefaultIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        t = createTable(
            "schema", "t",
            "id int not null",
            "x int",
            "primary key(id)");
        createIndex("schema", "t", "tx", "x");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        txIndexRowType = indexType(t, "x");
        tRowType = schema.userTableRowType(userTable(t));
        groupTable = group(t);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        db = new NewRow[]{
            createNewRow(t, 1000L, 8L),
            createNewRow(t, 1001L, 9L),
            createNewRow(t, 1002L, 8L),
            createNewRow(t, 1003L, 9L),
            createNewRow(t, 1004L, 8L),
            createNewRow(t, 1005L, 9L),
            createNewRow(t, 1006L, 8L),
            createNewRow(t, 1007L, 9L),
        };
        use(db);
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testNullInput1()
    {
        unionAll(null, tRowType, groupScan_Default(groupTable), tRowType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullInput1Type()
    {
        unionAll(groupScan_Default(groupTable), null, groupScan_Default(groupTable), tRowType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullInput2()
    {
        unionAll(groupScan_Default(groupTable), tRowType, null, tRowType);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullInput2Type()
    {
        unionAll(groupScan_Default(groupTable), tRowType, groupScan_Default(groupTable), null);
    }

    // Test operator execution

    @Test
    public void testBothEmpty()
    {
        Operator plan =
            unionAll(
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.literal(false)),
                tRowType,
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.literal(false)),
                tRowType);
        RowBase[] expected = new RowBase[]{};
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testLeftEmpty()
    {
        Operator plan =
            unionAll(
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.literal(false)),
                tRowType,
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(9), castResolver())),
                    tRowType);
        RowBase[] expected = new RowBase[]{
            row(tRowType, 1001L, 9L),
            row(tRowType, 1003L, 9L),
            row(tRowType, 1005L, 9L),
            row(tRowType, 1007L, 9L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testRightEmpty()
    {
        Operator plan =
            unionAll(
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(8), castResolver())),
                tRowType,
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.literal(false)),
                    tRowType);
        RowBase[] expected = new RowBase[]{
            row(tRowType, 1000L, 8L),
            row(tRowType, 1002L, 8L),
            row(tRowType, 1004L, 8L),
            row(tRowType, 1006L, 8L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testBothNonEmpty()
    {
        Operator plan =
            unionAll(
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(8), castResolver())),
                tRowType,
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(9), castResolver())),
                    tRowType);
        RowBase[] expected = new RowBase[]{
            row(tRowType, 1000L, 8L),
            row(tRowType, 1002L, 8L),
            row(tRowType, 1004L, 8L),
            row(tRowType, 1006L, 8L),
            row(tRowType, 1001L, 9L),
            row(tRowType, 1003L, 9L),
            row(tRowType, 1005L, 9L),
            row(tRowType, 1007L, 9L),
        };
        compareRows(expected, cursor(plan, queryContext));
    }

    @Test
    public void testCursor()
    {
        Operator plan =
            unionAll(
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(8), castResolver())),
                tRowType,
                select_HKeyOrdered(
                    groupScan_Default(groupTable),
                    tRowType,
                    ExpressionGenerators.compare(
                        ExpressionGenerators.field(tRowType, 1),
                        Comparison.EQ,
                        ExpressionGenerators.literal(9), castResolver())),
                tRowType);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(tRowType, 1000L, 8L),
                    row(tRowType, 1002L, 8L),
                    row(tRowType, 1004L, 8L),
                    row(tRowType, 1006L, 8L),
                    row(tRowType, 1001L, 9L),
                    row(tRowType, 1003L, 9L),
                    row(tRowType, 1005L, 9L),
                    row(tRowType, 1007L, 9L),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    // Inspired by bug972748. Also tests handling of index rows
    @Test
    public void testUADReuse()
    {
        IndexBound eight = new IndexBound(row(txIndexRowType, 8), new SetColumnSelector(0));
        IndexBound nine = new IndexBound(row(txIndexRowType, 9), new SetColumnSelector(0));
        IndexKeyRange xEQ8Range = IndexKeyRange.bounded(txIndexRowType, eight, true, eight, true);
        IndexKeyRange xEQ9Range = IndexKeyRange.bounded(txIndexRowType, nine, true, nine, true);
        Ordering ordering = new Ordering();
        ordering.append(field(txIndexRowType, 0), true);
        Operator plan =
            map_NestedLoops(
                limit_Default(
                    groupScan_Default(groupTable),
                    2),
                unionAll(
                    indexScan_Default(txIndexRowType,
                                      xEQ8Range,
                                      ordering),
                    txIndexRowType,
                    indexScan_Default(txIndexRowType,
                                      xEQ9Range,
                                      ordering),
                    txIndexRowType),
                0);
        String[] expected = new String[]{
            hKey(1000),
            hKey(1002),
            hKey(1004),
            hKey(1006),
            hKey(1001),
            hKey(1003),
            hKey(1005),
            hKey(1007),
            hKey(1000),
            hKey(1002),
            hKey(1004),
            hKey(1006),
            hKey(1001),
            hKey(1003),
            hKey(1005),
            hKey(1007),
        };
        compareRenderedHKeys(expected, cursor(plan, queryContext));
    }

    private String hKey(int id)
    {
        return String.format("{1,(long)%s}", id);
    }

    private int t;
    private UserTableRowType tRowType;
    private IndexRowType txIndexRowType;
    private Group groupTable;
}
