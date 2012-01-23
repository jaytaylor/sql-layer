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

package com.akiban.server.test.pt.qp;

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.UndefBindings;
import com.akiban.qp.operator.UpdateFunction;
import com.akiban.qp.row.OverlayingRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.store.PersistitStore;
import com.akiban.util.tap.Tap;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.Callable;

import static com.akiban.qp.operator.API.*;

public class HKeyChangePropagationProfilePT extends QPProfilePTBase
{
    @Before
    public void before() throws InvalidOperationException
    {
        // Changes to parent.gid propagate to children hkeys.
        grandparent = createTable(
            "schema", "grandparent",
            "gid int not null key",
            "gid_copy int," +
            "index(gid_copy)");
        parent = createTable(
            "schema", "parent",
            "pid int not null key",
            "gid int",
            "pid_copy int," +
            "index(pid_copy)",
            "constraint __akiban_pg foreign key __akiban_pg(gid) references grandparent(gid)");
        child1 = createTable(
            "schema", "child1",
            "cid1 int not null key",
            "pid int",
            "cid1_copy int," +
            "index(cid1_copy)",
            "constraint __akiban_c1p foreign key __akiban_c1p(pid) references parent(pid)");
        child2 = createTable(
            "schema", "child2",
            "cid2 int not null key",
            "pid int",
            "cid2_copy int," +
            "index(cid2_copy)",
            "constraint __akiban_c2p foreign key __akiban_c2p(pid) references parent(pid)");
        schema = new Schema(rowDefCache().ais());
        grandparentRowType = schema.userTableRowType(userTable(grandparent));
        parentRowType = schema.userTableRowType(userTable(parent));
        child1RowType = schema.userTableRowType(userTable(child1));
        child2RowType = schema.userTableRowType(userTable(child2));
        group = groupTable(grandparent);
        adapter = persistitAdapter(schema);
    }

    private int        grandparent;
    private int        parent;
    private int child1;
    private int        child2;
    private RowType    grandparentRowType;
    private RowType    parentRowType;
    private RowType    child1RowType;
    private RowType    child2RowType;
    private GroupTable group;

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
                            dml().writeRow(session(), createNewRow(parent, pid, gid, pid));
                            for (int i = 0; i < childrenPerParent; i++) {
                                dml().writeRow(session(), createNewRow(child1, cid, pid, cid));
                                dml().writeRow(session(), createNewRow(child2, cid, pid, cid));
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

    @Override
    protected void relevantTaps(TapsRegexes tapsRegexes)
    {
        tapsRegexes.add(".*propagate.*");
    }

    @Test
    public void profileHKeyChangePropagation() throws Exception
    {
        final int WARMUP_SCANS = 10; // Number of times to update each parent.gid during warmup
        final int SCANS = 100; // Number of times to update each parent.gid
        final int GRANDPARENTS = 1;
        final int PARENTS_PER_GRANDPARENT = 10;
        final int CHILDREN_PER_PARENT = 100;
        populateDB(GRANDPARENTS, PARENTS_PER_GRANDPARENT, CHILDREN_PER_PARENT);
        Operator scanPlan =
            filter_Default(
                groupScan_Default(group),
                Collections.singleton(parentRowType));
        final UpdatePlannable updatePlan =
            update_Default(scanPlan,
                           new UpdateFunction()
                           {
                               @Override
                               public Row evaluate(Row original, Bindings bindings)
                               {
                                   OverlayingRow updatedRow = new OverlayingRow(original);
                                   updatedRow.overlay(1, original.eval(1).getInt() - 1000000);
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
                        updatePlan.run(NO_BINDINGS, adapter);
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
        System.out.println(String.format("PDG optimization: %s", PersistitStore.PDG_OPTIMIZATION));
        System.out.println(String.format("scans: %s, db: %s/%s/%s, time: %s",
                                         SCANS, GRANDPARENTS, PARENTS_PER_GRANDPARENT, CHILDREN_PER_PARENT, sec));
    }

    private static final Bindings NO_BINDINGS = UndefBindings.only();
}
