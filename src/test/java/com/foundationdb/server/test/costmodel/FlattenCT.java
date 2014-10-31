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
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.server.error.InvalidOperationException;
import org.junit.Test;

import static com.foundationdb.qp.operator.API.*;

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
        schema = new Schema(ais());
        parentRowType = schema.tableRowType(table(parent));
        childRowType = schema.tableRowType(table(child));
        parentPKIndexType = indexType(parent, "pid", "parent_instance");
        group = group(parent);
        adapter = newStoreAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    protected void populateDB()
    {
        int cid = 0;
        for (int childCount : CHILD_COUNTS) {
            for (int i = 0; i < PARENT_INSTANCES; i++) {
                writeRow(parent, childCount, i);
                for (int c = 0; c < childCount; c++) {
                    writeRow(child, cid++, childCount, i);
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
                InputPreservationOption.DISCARD_INPUT);
        TimeOperator timeSetup = new TimeOperator(setup);
        Operator plan =
            flatten_HKeyOrdered(
                timeSetup,
                parentRowType,
                childRowType,
                joinType);
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(plan, queryContext, queryBindings);
            cursor.openTopLevel();
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
    private TableRowType parentRowType;
    private TableRowType childRowType;
    private IndexRowType parentPKIndexType;
    private Group group;
}
