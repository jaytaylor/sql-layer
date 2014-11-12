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

package com.foundationdb.server.test.costmodel;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.expression.IndexBound;
import com.foundationdb.qp.expression.IndexKeyRange;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.TimeOperator;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.error.InvalidOperationException;
import org.junit.Test;

import java.util.Collections;

import static com.foundationdb.qp.operator.API.*;

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
        schema = new Schema(ais());
        rootRowType = schema.tableRowType(table(root));
        oneRowType = schema.tableRowType(table(one));
        manyRowType = schema.tableRowType(table(many));
        rootPKIndexType = indexType(root, "rid", "root_instance");
        group = group(root);
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected void populateDB()
    {
        int cid = 0;
        for (int childCount : CHILD_COUNTS) {
            for (int i = 0; i < ROOT_INSTANCES; i++) {
                writeRow(root, childCount, i);
                writeRow(one, cid++, childCount, i);
                for (int c = 0; c < childCount; c++) {
                    writeRow(many, cid++, childCount, i);
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
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
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
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
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
        Cursor cursor = cursor(plan, queryContext, queryBindings);
        cursor.openTopLevel();
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
    private TableRowType rootRowType;
    private TableRowType oneRowType;
    private TableRowType manyRowType;
    private IndexRowType rootPKIndexType;
    private Group group;
}
