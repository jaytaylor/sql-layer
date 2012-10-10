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

import com.akiban.ais.model.Index;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.expression.RowBasedUnboundExpressions;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.qp.util.ValueSourceHasher;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.collation.AkCollatorFactory;
import com.akiban.server.collation.AkCollatorMySQL;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.types.AkType;
import com.persistit.Key;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.akiban.qp.operator.API.*;
import static org.junit.Assert.assertTrue;

// Like Select_BloomFilterIT, but testing with case-insensitive strings

public class Select_BloomFilter_CaseInsensitive_IT extends OperatorITBase
{
    @Before
    public void before()
    {
        AkCollatorMySQL.useForTesting();
        // Tables are Driving (D) and Filtering (F). Find Filtering rows with a given test id, yielding
        // a set of (a, b) rows. Then find Driving rows matching a and b.
        d = createTable(
            "schema", "driving",
            "test_id int not null",
            "a varchar(10) collate latin1_swedish_ci",
            "b varchar(10) collate latin1_swedish_ci");
        f = createTable(
            "schema", "filtering",
            "test_id int not null",
            "a varchar(10) collate latin1_swedish_ci",
            "b varchar(10) collate latin1_swedish_ci");
        Index dIndex = createIndex("schema", "driving", "idx_d", "test_id", "a", "b");
        Index fab = createIndex("schema", "filtering", "idx_fab", "a", "b");
        schema = new Schema(ais());
        dRowType = schema.userTableRowType(userTable(d));
        fRowType = schema.userTableRowType(userTable(f));
        dIndexRowType = dRowType.indexRowType(dIndex);
        fabIndexRowType = fRowType.indexRowType(fab);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        ciCollator = dRowType.userTable().getColumn("a").getCollator();
        db = new NewRow[]{
            // Test 0: No d or f rows
            // Test 1: No f rows
            createNewRow(f, 1L, "xy", 100L),
            // Test 2: No d rows
            createNewRow(f, 2L, "xy", 200L),
            // Test 3: 1 d row, no matching f rows
            createNewRow(d, 3L, "xy", "A"),
            createNewRow(f, 3L, "xz", "A"),
            createNewRow(f, 3L, "xy", "B"),
            // Test 4: 1 d row, 1 matching f rows
            createNewRow(d, 4L, "xy", "a"),
            createNewRow(f, 4L, "XY", "A"),
            createNewRow(f, 4L, "XZ", "A"),
            createNewRow(f, 4L, "XY", "B"),
            // Test 5: multiple d rows, no matching f rows
            createNewRow(d, 5L, "xy", "a"),
            createNewRow(d, 5L, "xz", "b"),
            createNewRow(f, 5L, "XY", "B"),
            createNewRow(f, 5L, "XZ", "A"),
            // Test 6: multiple d rows, multiple f rows, multiple matches
            createNewRow(d, 6L, "xy", "A"),
            createNewRow(d, 6L, "XY", "aB"),
            createNewRow(d, 6L, "Xy", "Ac"),
            createNewRow(d, 6L, "xY", "a"),
            createNewRow(f, 6L, "XY", "aZ"),
            createNewRow(f, 6L, "xy",  "Ab"),
            createNewRow(f, 6L, "Xy", "ac"),
            createNewRow(f, 6L, "xy", "aZ"),
            // Test 7: Null columns in d
            createNewRow(d, 7L, null, null),
            // Test 8: Null columns in f
            createNewRow(f, 8L, null, null),
        };
        use(db);
    }

    // Test ValueSourceHasher

    @Test
    public void testValueSourceHasher()
    {
        AkCollator caseInsensitiveCollator = AkCollatorFactory.getAkCollator("latin1_swedish_ci");
        AkCollator binaryCollator = AkCollatorFactory.getAkCollator(AkCollatorFactory.UCS_BINARY);
        PersistitKeyValueSource source = new PersistitKeyValueSource();
        long hash_AB;
        long hash_ab;
        Key key = adapter.newKey();
        {
            binaryCollator.append(key.clear(), "AB");
            source.attach(key, 0, AkType.VARCHAR, binaryCollator);
            hash_AB = ValueSourceHasher.hash(adapter, source, binaryCollator);
            binaryCollator.append(key.clear(), "ab");
            source.attach(key, 0, AkType.VARCHAR, binaryCollator);
            hash_ab = ValueSourceHasher.hash(adapter, source, binaryCollator);
            assertTrue(hash_AB != hash_ab);

        }
        {
            caseInsensitiveCollator.append(key.clear(), "AB");
            source.attach(key, 0, AkType.VARCHAR, caseInsensitiveCollator);
            hash_AB = ValueSourceHasher.hash(adapter, source, caseInsensitiveCollator);
            caseInsensitiveCollator.append(key.clear(), "ab");
            source.attach(key, 0, AkType.VARCHAR, caseInsensitiveCollator);
            hash_ab = ValueSourceHasher.hash(adapter, source, caseInsensitiveCollator);
            assertTrue(hash_AB == hash_ab);
        }
    }

    // Test operator execution

    @Test
    public void test0()
    {
        Operator plan = plan(0);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext), null, ciCollator, ciCollator);
    }

    @Test
    public void test1()
    {
        Operator plan = plan(1);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext), null, ciCollator, ciCollator);
    }

    @Test
    public void test2()
    {
        Operator plan = plan(2);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext), null, ciCollator, ciCollator);
    }

    @Test
    public void test3()
    {
        Operator plan = plan(3);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext), null, ciCollator, ciCollator);
    }

    @Test
    public void test4()
    {
        Operator plan = plan(4);
        RowBase[] expected = new RowBase[] {
            row(outputRowType, 4L, "xy", "a"),
        };
        compareRows(expected, cursor(plan, queryContext), null, ciCollator, ciCollator);
    }

    @Test
    public void test5()
    {
        Operator plan = plan(5);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext), null, ciCollator, ciCollator);
    }

    @Test
    public void test6()
    {
        Operator plan = plan(6);
        RowBase[] expected = new RowBase[] {
            row(outputRowType, 6L, "xy", "ab"),
            row(outputRowType, 6L, "xy", "ac"),
        };
        compareRows(expected, cursor(plan, queryContext), null, ciCollator, ciCollator);
    }

    @Test
    public void test7()
    {
        Operator plan = plan(7);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext), null, ciCollator, ciCollator);
    }

    @Test
    public void test8()
    {
        Operator plan = plan(8);
        RowBase[] expected = new RowBase[] {
        };
        compareRows(expected, cursor(plan, queryContext), null, ciCollator, ciCollator);
    }

    @Test
    public void testCursor()
    {
        Operator plan = plan(6);
        CursorLifecycleTestCase testCase = new CursorLifecycleTestCase()
        {
            @Override
            public RowBase[] firstExpectedRows()
            {
                return new RowBase[] {
                    row(outputRowType, 6L, "xy", "ab"),
                    row(outputRowType, 6L, "xy", "ac"),
                };
            }
        };
        testCursorLifecycle(plan, testCase, null, ciCollator, ciCollator);
    }

    public Operator plan(long testId)
    {
        List<AkCollator> collators = Arrays.asList(ciCollator, ciCollator);
        // loadFilter loads the filter with F rows containing the given testId.
        Operator loadFilter = project_Default(
            select_HKeyOrdered(
                filter_Default(
                    groupScan_Default(group(f)),
                    Collections.singleton(fRowType)),
                fRowType,
                Expressions.compare(
                    Expressions.field(fRowType, 0),
                    Comparison.EQ,
                    Expressions.literal(testId))),
            fRowType,
            Arrays.asList(Expressions.field(fRowType, 1),
                          Expressions.field(fRowType, 2)));
        // For the index scan retriving rows from the D(test_id) index
        IndexBound testIdBound =
            new IndexBound(row(dIndexRowType, testId), new SetColumnSelector(0));
        IndexKeyRange dTestIdKeyRange =
            IndexKeyRange.bounded(dIndexRowType, testIdBound, true, testIdBound, true);
        // For the  index scan retrieving rows from the F(a, b) index given a D index row
        IndexBound abBound = new IndexBound(
            new RowBasedUnboundExpressions(
                loadFilter.rowType(),
                Arrays.asList(
                    Expressions.boundField(dIndexRowType, 0, 1),
                    Expressions.boundField(dIndexRowType, 0, 2))),
            new SetColumnSelector(0, 1));
        IndexKeyRange fabKeyRange =
            IndexKeyRange.bounded(fabIndexRowType, abBound, true, abBound, true);
        // Use a bloom filter loaded by loadFilter. Then for each input row, check the filter (projecting
        // D rows on (a, b)), and, for positives, check F using an index scan keyed by D.a and D.b.
        Operator plan =
            project_Default(
                using_BloomFilter(
                    // filterInput
                    loadFilter,
                    // filterRowType
                    loadFilter.rowType(),
                    // estimatedRowCount
                    10,
                    // filterBindingPosition
                    0,
                    // streamInput
                    select_BloomFilter(
                        // input
                        indexScan_Default(dIndexRowType, dTestIdKeyRange, new Ordering()),
                        // onPositive
                        indexScan_Default(
                            fabIndexRowType,
                            fabKeyRange,
                            new Ordering()),
                        // filterFields
                        Arrays.asList(
                            Expressions.field(dIndexRowType, 1),
                            Expressions.field(dIndexRowType, 2)),
                        null,
                        // collators
                        collators,
                        // filterBindingPosition
                        0),
                    // collators
                    collators,
                    // usePValues
                    false
                    ),
                dIndexRowType,
                Arrays.asList(
                    Expressions.field(dIndexRowType, 0),   // test_id
                    Expressions.field(dIndexRowType, 1),   // a
                    Expressions.field(dIndexRowType, 2))); // b
        outputRowType = plan.rowType();
        return plan;
    }

    private int d;
    private int f;
    private UserTableRowType dRowType;
    private UserTableRowType fRowType;
    private RowType outputRowType;
    IndexRowType dIndexRowType;
    IndexRowType fabIndexRowType;
}
