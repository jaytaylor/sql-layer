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

package com.akiban.server.test.costmodel;

import com.akiban.ais.model.Group;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.TimeOperator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.error.InvalidOperationException;
import org.junit.Test;

import java.util.Collections;

import static com.akiban.qp.operator.API.*;

public class ProductCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB();
        runOneMany(WARMUP_RUNS, 4, false);
        for (int childCount : CHILD_COUNTS) {
            runOneMany(MEASURED_RUNS, childCount, true);
            runManyOne(MEASURED_RUNS, childCount, true);
        }
    }

    private void createSchema() throws InvalidOperationException
    {
        String schemaName = schemaName();
        String rootTableName = newTableName();
        root = createTable(schemaName, rootTableName,
                           "rid int not null",
                           "root_instance int not null",
                           "primary key(rid, root_instance)");
        String oneTableName = newTableName();
        one = createTable(schemaName, oneTableName,
                          "oid int not null",
                          "rid int",
                          "root_instance int",
                          "primary key(oid)",
                          String.format("grouping foreign key(rid, root_instance) references %s(rid, root_instance)",
                                        rootTableName));
        String manyTableName = newTableName();
        many = createTable(schemaName, manyTableName,
                           "mid int not null",
                           "rid int",
                           "root_instance int",
                           "primary key(mid)",
                           String.format("grouping foreign key(rid, root_instance) references %s(rid, root_instance)",
                                         rootTableName));
        schema = new Schema(rowDefCache().ais());
        rootRowType = schema.userTableRowType(userTable(root));
        oneRowType = schema.userTableRowType(userTable(one));
        manyRowType = schema.userTableRowType(userTable(many));
        rootPKIndexType = indexType(root, "rid", "root_instance");
        group = group(root);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    protected void populateDB()
    {
        int cid = 0;
        for (int childCount : CHILD_COUNTS) {
            for (int i = 0; i < ROOT_INSTANCES; i++) {
                dml().writeRow(session(), createNewRow(root, childCount, i));
                dml().writeRow(session(), createNewRow(one, cid++, childCount, i));
                for (int c = 0; c < childCount; c++) {
                    dml().writeRow(session(), createNewRow(many, cid++, childCount, i));
                }
            }
        }
    }

    private void runOneMany(int runs, int childCount, boolean report)
    {
        IndexBound rid = new IndexBound(row(rootPKIndexType, childCount), new SetColumnSelector(0));
        IndexKeyRange rootRidRange = IndexKeyRange.bounded(rootPKIndexType, rid, true, rid, true);
        Operator outerPlan =
            flatten_HKeyOrdered(
                branchLookup_Default(
                    ancestorLookup_Default(
                        indexScan_Default(rootPKIndexType, false, rootRidRange),
                        group,
                        rootPKIndexType,
                        Collections.singleton(rootRowType),
                        InputPreservationOption.DISCARD_INPUT),
                    group,
                    rootRowType,
                    oneRowType,
                    InputPreservationOption.KEEP_INPUT),
                rootRowType,
                oneRowType,
                JoinType.INNER_JOIN);
        Operator innerPlan =
            flatten_HKeyOrdered(
                branchLookup_Nested(
                    group,
                    rootRowType,
                    rootRowType,
                    manyRowType,
                    InputPreservationOption.KEEP_INPUT,
                    0),
                rootRowType,
                manyRowType,
                JoinType.INNER_JOIN);
        TimeOperator timedOuter = new TimeOperator(outerPlan);
        TimeOperator timedInner = new TimeOperator(innerPlan);
        Operator plan =
            product_NestedLoops(
                timedOuter,
                timedInner,
                outerPlan.rowType(),
                rootRowType,
                innerPlan.rowType(),
                0);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(plan, queryContext);
            cursor.open();
            while (cursor.next() != null);
        }
        long stop = System.nanoTime();
        long productNsec = stop - start - timedOuter.elapsedNsec() - timedInner.elapsedNsec();
        if (report) {
            // Report the difference
            double averageUsecPerRow = productNsec / (1000.0 * runs * (childCount + 1));
            System.out.println(String.format("one->many childCount = %s: %s usec/row",
                                             childCount, averageUsecPerRow));
        }
    }

    private void runManyOne(int runs, int childCount, boolean report)
    {
        IndexBound rid = new IndexBound(row(rootPKIndexType, childCount), new SetColumnSelector(0));
        IndexKeyRange rootRidRange = IndexKeyRange.bounded(rootPKIndexType, rid, true, rid, true);
        Operator outerPlan =
            flatten_HKeyOrdered(
                branchLookup_Default(
                    ancestorLookup_Default(
                        indexScan_Default(rootPKIndexType, false, rootRidRange),
                        group,
                        rootPKIndexType,
                        Collections.singleton(rootRowType),
                        InputPreservationOption.DISCARD_INPUT),
                    group,
                    rootRowType,
                    manyRowType,
                    InputPreservationOption.KEEP_INPUT),
                rootRowType,
                manyRowType,
                JoinType.INNER_JOIN);
        Operator innerPlan =
            flatten_HKeyOrdered(
                branchLookup_Nested(
                    group,
                    rootRowType,
                    rootRowType,
                    oneRowType,
                    InputPreservationOption.KEEP_INPUT,
                    0),
                rootRowType,
                oneRowType,
                JoinType.INNER_JOIN);
        TimeOperator timedOuter = new TimeOperator(outerPlan);
        TimeOperator timedInner = new TimeOperator(innerPlan);
        Operator plan =
            product_NestedLoops(
                timedOuter,
                timedInner,
                outerPlan.rowType(),
                rootRowType,
                innerPlan.rowType(),
                0);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(plan, queryContext);
            cursor.open();
            while (cursor.next() != null);
        }
        long stop = System.nanoTime();
        long productNsec = stop - start - timedOuter.elapsedNsec() - timedInner.elapsedNsec();
        if (report) {
            // Report the difference
            double averageUsecPerRow = productNsec / (1000.0 * runs * (childCount + 1));
            System.out.println(String.format("many->one childCount = %s: %s usec/row",
                                             childCount, averageUsecPerRow));
        }
    }

    private void dump(Operator plan)
    {
        Cursor cursor = cursor(plan, queryContext);
        cursor.open();
        Row row;
        while ((row = cursor.next()) != null) {
            System.out.println(row);
        }
    }

    // The database has roots with varying numbers of children. For each such number, there are ROOT_INSTANCES
    // root rows.
    private static final int ROOT_INSTANCES = 100;
    private static final int WARMUP_RUNS = 10000;
    private static final int MEASURED_RUNS = 1000;
    private static final int[] CHILD_COUNTS = new int[]{64, 32, 16, 8, 4, 2, 1, 0};

    private int root;
    private int one;
    private int many;
    private UserTableRowType rootRowType;
    private UserTableRowType oneRowType;
    private UserTableRowType manyRowType;
    private IndexRowType rootPKIndexType;
    private Group group;
}
