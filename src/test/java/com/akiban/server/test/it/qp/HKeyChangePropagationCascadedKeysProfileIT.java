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

package com.akiban.server.test.it.qp;

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
import com.akiban.util.Tap;
import com.akiban.util.TapReport;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;

import static com.akiban.qp.operator.API.*;

public class HKeyChangePropagationCascadedKeysProfileIT extends QPProfileITBase
{
    @Before
    public void before() throws InvalidOperationException
    {
        // Changes to parent.gid propagate to children hkeys.
        grandparent = createTable(
            "schema", "grandparent",
            "gid int not null",
            "gid_copy int",
            "primary key(gid)",
            "index(gid_copy)");
        parent = createTable(
            "schema", "parent",
            "gid int not null",
            "pid int not null",
            "pid_copy int",
            "index(pid_copy)",
            "primary key(gid, pid)",
            "constraint __akiban_pg foreign key __akiban_pg(gid) references grandparent(gid)");
        child1 = createTable(
            "schema", "child1",
            "gid int not null",
            "pid int not null",
            "cid1 int not null",
            "cid1_copy int",
            "index(cid1_copy)",
            "primary key(gid, pid, cid1)",
            "constraint __akiban_c1p foreign key __akiban_c1p(gid, pid) references parent(gid, pid)");
        child2 = createTable(
            "schema", "child2",
            "gid int not null",
            "pid int not null",
            "cid2 int not null",
            "cid2_copy int",
            "index(cid2_copy)",
            "primary key(gid, pid, cid2)",
            "constraint __akiban_c2p foreign key __akiban_c2p(gid, pid) references parent(gid, pid)");
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

    protected void populateDB(int grandparents, int parentsPerGrandparent, int childrenPerParent)
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
    }

    @Test
    @Ignore
    public void profileHKeyChangePropagation() throws PersistitException
    {
        final int WARMUP_SCANS = 10; // Number of times to update each parent.gid during warmup
        final int SCANS = 100; // Number of times to update each parent.gid
        final int GRANDPARENTS = 1;
        final int PARENTS_PER_GRANDPARENT = 10;
        final int CHILDREN_PER_PARENT = 10;
        populateDB(GRANDPARENTS, PARENTS_PER_GRANDPARENT, CHILDREN_PER_PARENT);
        // Change gid of every row of every type
        Operator scanPlan = groupScan_Default(group);
        UpdatePlannable updatePlan =
            update_Default(scanPlan,
                           new UpdateFunction()
                           {
                               @Override
                               public Row evaluate(Row original, Bindings bindings)
                               {
                                   OverlayingRow updatedRow = new OverlayingRow(original);
                                   updatedRow.overlay(0, original.eval(0).getInt() - 1000000);
                                   return updatedRow;
                               }

                               @Override
                               public boolean rowIsSelected(Row row)
                               {
                                   return true;
                               }
                           });
        Transaction transaction = treeService().getTransaction(session());
        transaction.begin();
        long start = Long.MIN_VALUE;
        for (int s = 0; s < WARMUP_SCANS + SCANS; s++) {
            if (s == WARMUP_SCANS) {
                Tap.setEnabled(".*propagate.*", true);
                start = System.nanoTime();
            }
            updatePlan.run(NO_BINDINGS, adapter);
            transaction.commit();
            transaction.end();
            transaction.begin();
        }
        transaction.commit();
        transaction.end();
        long end = System.nanoTime();
        double sec = (end - start) / (1000.0 * 1000 * 1000);
        System.out.println(String.format("PDG optimization: %s", PersistitStore.PDG_OPTIMIZATION));
        System.out.println(String.format("scans: %s, db: %s/%s/%s, time: %s",
                                         SCANS, GRANDPARENTS, PARENTS_PER_GRANDPARENT, CHILDREN_PER_PARENT, sec));
        TapReport[] propagateReport = Tap.getReport(".*propagate.*");
        for (TapReport report : propagateReport) {
            System.out.println(String.format("%s: %s", report.getName(), report.getInCount()));
        }
    }

    private static final Bindings NO_BINDINGS = UndefBindings.only();
}
