/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.costmodel;

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.TimeOperator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.expression.std.Expressions;
import com.akiban.server.expression.std.FieldExpression;
import org.junit.Test;

import static com.akiban.qp.operator.API.*;

public class FlattenCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB();
        for (JoinType joinType : JoinType.values()) {
            run(WARMUP_RUNS, 4, joinType, false);
            for (int childCount : CHILD_COUNTS) {
                run(MEASURED_RUNS, childCount, joinType, true);
            }
        }
    }

    private void createSchema() throws InvalidOperationException
    {
        String schemaName = schemaName();
        String parentTableName = newTableName();
        parent = createTable(schemaName, parentTableName,
                        "pid int not null",
                        "parent_instance int not null",
                        "primary key(pid, parent_instance)");
        String childTableName = newTableName();
        child = createTable(schemaName, childTableName,
                            "cid int not null",
                            "pid int",
                            "parent_instance int",
                            "primary key(cid)",
                            String.format("grouping foreign key(pid, parent_instance) references %s(pid, parent_instance)", 
                                          parentTableName));
        schema = new Schema(rowDefCache().ais());
        parentRowType = schema.userTableRowType(userTable(parent));
        childRowType = schema.userTableRowType(userTable(child));
        parentPKIndexType = indexType(parent, "pid", "parent_instance");
        group = groupTable(parent);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    protected void populateDB()
    {
        int cid = 0;
        for (int childCount : CHILD_COUNTS) {
            for (int i = 0; i < PARENT_INSTANCES; i++) {
                dml().writeRow(session(), createNewRow(parent, childCount, i));
                for (int c = 0; c < childCount; c++) {
                    dml().writeRow(session(), createNewRow(child, cid++, childCount, i));
                }
            }
        }
    }
    
    private void run(int runs, int childCount, JoinType joinType, boolean report)
    {
        IndexBound pid = new IndexBound(row(parentPKIndexType, childCount), new SetColumnSelector(0));
        IndexKeyRange pidRange = IndexKeyRange.bounded(parentPKIndexType, pid, true, pid, true);
        Operator setup =
            branchLookup_Default(
                indexScan_Default(parentPKIndexType, false, pidRange),
                group,
                parentPKIndexType,
                parentRowType,
                LookupOption.DISCARD_INPUT);
        TimeOperator timeSetup = new TimeOperator(setup);
        Operator plan =
            flatten_HKeyOrdered(
                timeSetup,
                parentRowType,
                childRowType,
                joinType);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(plan, queryContext);
            cursor.open();
            while (cursor.next() != null);
        }
        long stop = System.nanoTime();
        long flattenNsec = stop - start - timeSetup.elapsedNsec();
        if (report) {
            // Report the difference
            double averageUsecPerRow = flattenNsec / (1000.0 * runs * (childCount + 1));
            System.out.println(String.format("%s childCount = %s: %s usec/row",
                                             joinType, childCount, averageUsecPerRow));
        }
    }

    // The database has parents with varying numbers of children. For each such number, there are PARENT_INSTANCES
    // parent rows.
    private static final int PARENT_INSTANCES = 100;
    private static final int WARMUP_RUNS = 10000;
    private static final int MEASURED_RUNS = 1000;
    private static final int[] CHILD_COUNTS = new int[]{64, 32, 16, 8, 4, 2, 1, 0};

    private int parent;
    private int child;
    private UserTableRowType parentRowType;
    private UserTableRowType childRowType;
    private IndexRowType parentPKIndexType;
    private GroupTable group;
}
