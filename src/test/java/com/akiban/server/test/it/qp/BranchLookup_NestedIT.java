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
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static com.akiban.qp.operator.API.*;

public class BranchLookup_NestedIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        r = createTable(
            "schema", "r",
            "rid int not null primary key",
            "rvalue varchar(20)");
        createIndex("schema", "r", "rvalue", "rvalue");
        a = createTable(
            "schema", "a",
            "aid int not null primary key",
            "rid int",
            "avalue varchar(20)",
            "grouping foreign key(rid) references r(rid)");
        createIndex("schema", "a", "avalue", "avalue");
        b = createTable(
            "schema", "b",
            "bid int not null primary key",
            "rid int",
            "bvalue varchar(20)",
            "grouping foreign key(rid) references r(rid)");
        createIndex("schema", "b", "bvalue", "bvalue");
        c = createTable(
            "schema", "c",
            "cid int not null primary key",
            "rid int",
            "cvalue varchar(20)",
            "grouping foreign key(rid) references r(rid)");
        createIndex("schema", "c", "cvalue", "cvalue");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        rRowType = schema.userTableRowType(userTable(r));
        aRowType = schema.userTableRowType(userTable(a));
        bRowType = schema.userTableRowType(userTable(b));
        cRowType = schema.userTableRowType(userTable(c));
        rValueIndexRowType = indexType(r, "rvalue");
        aValueIndexRowType = indexType(a, "avalue");
        bValueIndexRowType = indexType(b, "bvalue");
        cValueIndexRowType = indexType(c, "cvalue");
        rabc = group(r);
        db = new NewRow[]{createNewRow(r, 1L, "r1"),
                          createNewRow(r, 2L, "r2"),
                          createNewRow(a, 13L, 1L, "a13"),
                          createNewRow(a, 14L, 1L, "a14"),
                          createNewRow(a, 23L, 2L, "a23"),
                          createNewRow(a, 24L, 2L, "a24"),
                          createNewRow(b, 15L, 1L, "b15"),
                          createNewRow(b, 16L, 1L, "b16"),
                          createNewRow(b, 25L, 2L, "b25"),
                          createNewRow(b, 26L, 2L, "b26"),
                          createNewRow(c, 17L, 1L, "c17"),
                          createNewRow(c, 18L, 1L, "c18"),
                          createNewRow(c, 27L, 2L, "c27"),
                          createNewRow(c, 28L, 2L, "c28"),
        };
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        use(db);
    }

    // Test argument validation

    @Test(expected = IllegalArgumentException.class)
    public void testBLNGroupTableNull()
    {
        branchLookup_Nested(null, aRowType, bRowType, InputPreservationOption.KEEP_INPUT, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBLNInputRowTypeNull()
    {
        branchLookup_Nested(rabc, null, bRowType, InputPreservationOption.KEEP_INPUT, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBLNOutputRowTypeNull()
    {
        branchLookup_Nested(rabc, aRowType, null, InputPreservationOption.KEEP_INPUT, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBLNLookupOptionNull()
    {
        branchLookup_Nested(rabc, aRowType, bRowType, null, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBLNBadInputBindingPosition()
    {
        branchLookup_Nested(rabc, aRowType, bRowType, InputPreservationOption.KEEP_INPUT, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBLNInputTypeNotDescendent()
    {
        branchLookup_Nested(rabc, aRowType, bRowType, bRowType, InputPreservationOption.KEEP_INPUT, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBLNOutputTypeNotDescendent()
    {
        branchLookup_Nested(rabc, aRowType, aRowType, bRowType, InputPreservationOption.KEEP_INPUT, -1);
    }

    // Test operator execution

    @Test
    public void testAIndexToR()
    {
        Operator plan =
            map_NestedLoops(
                indexScan_Default(aValueIndexRowType),
                branchLookup_Nested(rabc, aValueIndexRowType, rRowType, InputPreservationOption.DISCARD_INPUT, 0),
                0);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            // Each r row, and everything below it, is duplicated, because the A index refers to each r value twice.
            row(rRowType, 1L, "r1"),
            row(aRowType, 13L, 1L, "a13"),
            row(aRowType, 14L, 1L, "a14"),
            row(bRowType, 15L, 1L, "b15"),
            row(bRowType, 16L, 1L, "b16"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(rRowType, 1L, "r1"),
            row(aRowType, 13L, 1L, "a13"),
            row(aRowType, 14L, 1L, "a14"),
            row(bRowType, 15L, 1L, "b15"),
            row(bRowType, 16L, 1L, "b16"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(rRowType, 2L, "r2"),
            row(aRowType, 23L, 2L, "a23"),
            row(aRowType, 24L, 2L, "a24"),
            row(bRowType, 25L, 2L, "b25"),
            row(bRowType, 26L, 2L, "b26"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
            row(rRowType, 2L, "r2"),
            row(aRowType, 23L, 2L, "a23"),
            row(aRowType, 24L, 2L, "a24"),
            row(bRowType, 25L, 2L, "b25"),
            row(bRowType, 26L, 2L, "b26"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAToR()
    {
        Operator plan =
            map_NestedLoops(
                ancestorLookup_Default(
                    indexScan_Default(aValueIndexRowType),
                    rabc,
                    aValueIndexRowType,
                    Collections.singleton(aRowType),
                    InputPreservationOption.DISCARD_INPUT),
                branchLookup_Nested(rabc, aRowType, rRowType, InputPreservationOption.DISCARD_INPUT, 0),
                0);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            // Each r row, and everything below it, is duplicated, because the A index refers to each r value twice.
            row(rRowType, 1L, "r1"),
            row(aRowType, 13L, 1L, "a13"),
            row(aRowType, 14L, 1L, "a14"),
            row(bRowType, 15L, 1L, "b15"),
            row(bRowType, 16L, 1L, "b16"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(rRowType, 1L, "r1"),
            row(aRowType, 13L, 1L, "a13"),
            row(aRowType, 14L, 1L, "a14"),
            row(bRowType, 15L, 1L, "b15"),
            row(bRowType, 16L, 1L, "b16"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(rRowType, 2L, "r2"),
            row(aRowType, 23L, 2L, "a23"),
            row(aRowType, 24L, 2L, "a24"),
            row(bRowType, 25L, 2L, "b25"),
            row(bRowType, 26L, 2L, "b26"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
            row(rRowType, 2L, "r2"),
            row(aRowType, 23L, 2L, "a23"),
            row(aRowType, 24L, 2L, "a24"),
            row(bRowType, 25L, 2L, "b25"),
            row(bRowType, 26L, 2L, "b26"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAToB()
    {
        Operator plan =
            map_NestedLoops(
                filter_Default(
                    groupScan_Default(rabc),
                    Collections.singleton(aRowType)),
                branchLookup_Nested(rabc, aRowType, bRowType, InputPreservationOption.DISCARD_INPUT, 0),
                0);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(bRowType, 15L, 1L, "b15"),
            row(bRowType, 16L, 1L, "b16"),
            row(bRowType, 15L, 1L, "b15"),
            row(bRowType, 16L, 1L, "b16"),
            row(bRowType, 25L, 2L, "b25"),
            row(bRowType, 26L, 2L, "b26"),
            row(bRowType, 25L, 2L, "b25"),
            row(bRowType, 26L, 2L, "b26"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testAToBAndC()
    {
        Operator plan =
            map_NestedLoops(
                map_NestedLoops(
                    filter_Default(
                        groupScan_Default(rabc),
                        Collections.singleton(aRowType)),
                    branchLookup_Nested(rabc, aRowType, bRowType, InputPreservationOption.DISCARD_INPUT, 0),
                    0),
                branchLookup_Nested(rabc, bRowType, cRowType, InputPreservationOption.KEEP_INPUT, 1),
                1);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(bRowType, 15L, 1L, "b15"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(bRowType, 16L, 1L, "b16"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(bRowType, 15L, 1L, "b15"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(bRowType, 16L, 1L, "b16"),
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
            row(bRowType, 25L, 2L, "b25"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
            row(bRowType, 26L, 2L, "b26"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
            row(bRowType, 25L, 2L, "b25"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
            row(bRowType, 26L, 2L, "b26"),
            row(cRowType, 27L, 2L, "c27"),
            row(cRowType, 28L, 2L, "c28"),
        };
        compareRows(expected, cursor);
    }

    // Multiple index tests

    @Test
    public void testABIndexToC()
    {
        Operator abIndexScan =
            hKeyUnion_Ordered(
                indexScan_Default(aValueIndexRowType, false, aValueRange("a13", "a14")),
                indexScan_Default(bValueIndexRowType, false, bValueRange("b15", "b16")),
                aValueIndexRowType,
                bValueIndexRowType,
                2,
                2,
                2,
                rRowType);
        Operator plan =
            map_NestedLoops(
                abIndexScan,
                branchLookup_Nested(rabc, abIndexScan.rowType(), cRowType, InputPreservationOption.DISCARD_INPUT, 0),
                0);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
            row(cRowType, 17L, 1L, "c17"),
            row(cRowType, 18L, 1L, "c18"),
        };
        compareRows(expected, cursor);
    }
    
    @Test
    public void testAToA()
    {
        Operator plan =
                map_NestedLoops(
                        filter_Default(
                                groupScan_Default(rabc),
                                Collections.singleton(aRowType)),
                        branchLookup_Nested(rabc, aRowType, rRowType, aRowType, InputPreservationOption.DISCARD_INPUT, 0),
                        0);
        Cursor cursor = cursor(plan, queryContext);
        RowBase[] expected = new RowBase[]{
                row(aRowType, 13L, 1L, "a13"),
                row(aRowType, 14L, 1L, "a14"),
                row(aRowType, 13L, 1L, "a13"),
                row(aRowType, 14L, 1L, "a14"),
                row(aRowType, 23L, 2L, "a23"),
                row(aRowType, 24L, 2L, "a24"),
                row(aRowType, 23L, 2L, "a23"),
                row(aRowType, 24L, 2L, "a24"),
        };
        compareRows(expected, cursor);
    }

    @Test
    public void testCursor()
    {
        Operator plan =
            map_NestedLoops(
                filter_Default(
                    groupScan_Default(rabc),
                    Collections.singleton(aRowType)),
                branchLookup_Nested(rabc, aRowType, rRowType, aRowType, InputPreservationOption.DISCARD_INPUT, 0),
                0);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(aRowType, 13L, 1L, "a13"),
                    row(aRowType, 14L, 1L, "a14"),
                    row(aRowType, 13L, 1L, "a13"),
                    row(aRowType, 14L, 1L, "a14"),
                    row(aRowType, 23L, 2L, "a23"),
                    row(aRowType, 24L, 2L, "a24"),
                    row(aRowType, 23L, 2L, "a23"),
                    row(aRowType, 24L, 2L, "a24"),
                };
            }
        };
        testCursorLifecycle(plan, testCase);
    }

    private IndexKeyRange aValueRange(String lo, String hi)
    {
        return IndexKeyRange.bounded(aValueIndexRowType, aValueBound(lo), true, aValueBound(hi), true);
    }

    private IndexKeyRange bValueRange(String lo, String hi)
    {
        return IndexKeyRange.bounded(bValueIndexRowType, bValueBound(lo), true, bValueBound(hi), true);
    }

    private IndexBound aValueBound(String a)
    {
        return new IndexBound(row(aValueIndexRowType, a), new SetColumnSelector(0));
    }

    private IndexBound bValueBound(String b)
    {
        return new IndexBound(row(bValueIndexRowType, b), new SetColumnSelector(0));
    }

    protected int r;
    protected int a;
    protected int c;
    protected int b;
    protected UserTableRowType rRowType;
    protected UserTableRowType aRowType;
    protected UserTableRowType bRowType;
    protected UserTableRowType cRowType;
    protected IndexRowType rValueIndexRowType;
    protected IndexRowType aValueIndexRowType;
    protected IndexRowType bValueIndexRowType;
    protected IndexRowType cValueIndexRowType;
    protected Group rabc;
}
