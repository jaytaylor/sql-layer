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

package com.foundationdb.server.test.pt.qp;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.exec.UpdatePlannable;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.UpdateFunction;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.util.tap.Tap;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Callable;

import static com.foundationdb.qp.operator.API.groupScan_Default;
import static com.foundationdb.qp.operator.API.update_Default;

public class HKeyChangePropagationCascadedKeysProfilePT extends QPProfilePTBase
{
    @Before
    @Override
    public void setUpProfiling() throws Exception
    {
        // Changes to parent.gid propagate to children hkeys.
        grandparent = createTable(
            "schema", "grandparent",
            "gid int not null",
            "gid_copy int",
            "primary key(gid)");
        createIndex("schema", "grandparent", "idx_gid_copy", "gid_copy");
        parent = createTable(
            "schema", "parent",
            "gid int not null",
            "pid int not null",
            "pid_copy int",
            "primary key(gid, pid)",
            "grouping foreign key(gid) references grandparent(gid)");
        createIndex("schema", "parent", "idx_pid_copy", "pid_copy");
        child1 = createTable(
            "schema", "child1",
            "gid int not null",
            "pid int not null",
            "cid1 int not null",
            "cid1_copy int",
            "primary key(gid, pid, cid1)",
            "grouping foreign key(gid, pid) references parent(gid, pid)");
        createIndex("schema", "child1", "idx_cid1_copy", "cid1_copy");
        child2 = createTable(
            "schema", "child2",
            "gid int not null",
            "pid int not null",
            "cid2 int not null",
            "cid2_copy int",
            "primary key(gid, pid, cid2)",
            "grouping foreign key(gid, pid) references parent(gid, pid)");
        createIndex("schema", "child2", "idx_cid2_copy", "cid2_copy");
        schema = new Schema(ais());
        grandparentRowType = schema.tableRowType(table(grandparent));
        parentRowType = schema.tableRowType(table(parent));
        child1RowType = schema.tableRowType(table(child1));
        child2RowType = schema.tableRowType(table(child2));
        group = group(grandparent);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
        // The following is adapter from super.setUpProfiling. Leave taps disabled, they'll be enabled after loading
        // and warmup
        beforeProfiling();
        tapsRegexes.clear();
        registerTaps();
    }

    @Override
    protected void registerTaps()
    {
        tapsRegexes.add(".*propagate.*");
    }

    private int        grandparent;
    private int        parent;
    private int child1;
    private int        child2;
    private RowType    grandparentRowType;
    private RowType    parentRowType;
    private RowType    child1RowType;
    private RowType    child2RowType;
    private Group      group;

    protected void populateDB(final int grandparents, 
                              final int parentsPerGrandparent, 
                              final int childrenPerParent) throws Exception
    {
        transactionally(
            new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    long gid = 0;
                    long pid = 0;
                    long cid = 0;
                    for (int c = 0; c < grandparents; c++) {
                        dml().writeRow(session(), createNewRow(grandparent, gid, gid));
                        for (int o = 0; o < parentsPerGrandparent; o++) {
                            dml().writeRow(session(), createNewRow(parent, gid, pid, pid));
                            for (int i = 0; i < childrenPerParent; i++) {
                                dml().writeRow(session(), createNewRow(child1, gid, pid, cid, cid));
                                dml().writeRow(session(), createNewRow(child2, gid, pid, cid, cid));
                                cid++;
                            }
                            pid++;
                        }
                        gid++;
                    }
                    return null;
                }
            });
    }

    @Test
    public void profileHKeyChangePropagation() throws Exception
    {
        final int WARMUP_SCANS = 10; // Number of times to update each parent.gid during warmup
        final int SCANS = 100; // Number of times to update each parent.gid
        final int GRANDPARENTS = 1;
        final int PARENTS_PER_GRANDPARENT = 10;
        final int CHILDREN_PER_PARENT = 10;
        populateDB(GRANDPARENTS, PARENTS_PER_GRANDPARENT, CHILDREN_PER_PARENT);
        // Change gid of every row of every type
        Operator scanPlan = groupScan_Default(group);
        final UpdatePlannable updatePlan =
            update_Default(scanPlan,
                           new UpdateFunction()
                           {
                               @Override
                               public Row evaluate(Row original, QueryContext context, QueryBindings bindings)
                               {
                                   OverlayingRow updatedRow = new OverlayingRow(original);
                                   long i = original.value(0).getInt64();
                                   updatedRow.overlay(0, i - 1000000);
                                   return updatedRow;
                               }

                               @Override
                               public boolean rowIsSelected(Row row)
                               {
                                   return true;
                               }
                           });
        long start = Long.MIN_VALUE;
        for (int s = 0; s < WARMUP_SCANS + SCANS; s++) {
            final int sFinal = s;
            long mightBeStartTime = transactionally(
                new Callable<Long>()
                {
                    @Override
                    public Long call() throws Exception
                    {
                        long start = -1L;
                        if (sFinal == WARMUP_SCANS) {
                            Tap.setEnabled(".*propagate.*", true);
                            start = System.nanoTime();
                        }
                        updatePlan.run(queryContext, queryBindings);
                        return start;
                    }
                });
            if (mightBeStartTime != -1L) {
                start = mightBeStartTime;
            }
        }
        long end = System.nanoTime();
        assert start != Long.MIN_VALUE;
        double sec = (end - start) / (1000.0 * 1000 * 1000);
        System.out.println(String.format("scans: %s, db: %s/%s/%s, time: %s",
                                         SCANS, GRANDPARENTS, PARENTS_PER_GRANDPARENT, CHILDREN_PER_PARENT, sec));
    }
}
