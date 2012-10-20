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

package com.akiban.server.test.pt.qp;

import com.akiban.ais.model.Group;
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.UpdateFunction;
import com.akiban.qp.row.OverlayingRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.util.tap.Tap;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.Callable;

import static com.akiban.qp.operator.API.*;

public class HKeyChangePropagationProfilePT extends QPProfilePTBase
{
    @Before
    @Override
    public void setUpProfiling() throws Exception
    {
        // Changes to parent.gid propagate to children hkeys.
        grandparent = createTable(
            "schema", "grandparent",
            "gid int not null primary key",
            "gid_copy int");
        createIndex("schema", "grandparent", "idx_gid_copy", "gid_copy");
        parent = createTable(
            "schema", "parent",
            "pid int not null primary key",
            "gid int",
            "pid_copy int," +
            "grouping foreign key(gid) references grandparent(gid)");
        createIndex("schema", "parent", "idx_pid_copy", "pid_copy");
        child1 = createTable(
            "schema", "child1",
            "cid1 int not null primary key",
            "pid int",
            "cid1_copy int," +
            "grouping foreign key(pid) references parent(pid)");
        createIndex("schema", "child1", "idx_cid1_copy", "cid1_copy");
        child2 = createTable(
            "schema", "child2",
            "cid2 int not null primary key",
            "pid int",
            "cid2_copy int," +
            "grouping foreign key(pid) references parent(pid)");
        createIndex("schema", "child2", "idx_cid2_copy", "cid2_copy");
        schema = new Schema(ais());
        grandparentRowType = schema.userTableRowType(userTable(grandparent));
        parentRowType = schema.userTableRowType(userTable(parent));
        child1RowType = schema.userTableRowType(userTable(child1));
        child2RowType = schema.userTableRowType(userTable(child2));
        group = group(grandparent);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        // The following is adapter from super.setUpProfiling. Leave taps disabled, they'll be enabled after loading
        // and warmup
        beforeProfiling();
        tapsRegexes.clear();
        registerTaps();
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
    protected void registerTaps()
    {
        tapsRegexes.add(".*propagate.*");
    }

    @Test
    public void profileHKeyChangePropagationFromParent() throws Exception
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
                               public boolean usePValues() {
                                   return usingPValues();
                               }

                               @Override
                               public Row evaluate(Row original, QueryContext context)
                               {
                                   OverlayingRow updatedRow = new OverlayingRow(original);
                                   long i;
                                   if (usePValues()) {
                                       i = original.pvalue(1).getInt64();
                                   }
                                   else {
                                       i = getLong(original, 1);
                                   }
                                   updatedRow.overlay(1, i - 1000000);
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
                        updatePlan.run(queryContext);
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

    @Test
    public void profileHKeyChangePropagationFromGrandparent() throws Exception
    {
        final int WARMUP_SCANS = 10; // Number of times to update each parent.gid during warmup
        final int SCANS = 100; // Number of times to update each parent.gid
        final int GRANDPARENTS = 1;
        final int PARENTS_PER_GRANDPARENT = 10;
        final int CHILDREN_PER_PARENT = 100;
        populateDB(GRANDPARENTS, PARENTS_PER_GRANDPARENT, CHILDREN_PER_PARENT);
        Operator scanPlan =
            limit_Default(
                filter_Default(
                    groupScan_Default(group),
                    Collections.singleton(grandparentRowType)),
                1);
        final UpdatePlannable updatePlan =
            update_Default(scanPlan,
                           new UpdateFunction()
                           {
                               @Override
                               public boolean usePValues() {
                                   return usingPValues();
                               }

                               @Override
                               public Row evaluate(Row original, QueryContext context)
                               {
                                   OverlayingRow updatedRow = new OverlayingRow(original);
                                   long i;
                                   if (usePValues()) {
                                       i = original.pvalue(0).getInt64();
                                   }
                                   else {
                                       i = getLong(original, 0);
                                   }
                                   updatedRow.overlay(0, i - 1000000);
                                   return updatedRow;
                               }

                               @Override
                               public boolean rowIsSelected(Row row)
                               {
                                   return true;
                               }
                           });
        final UpdatePlannable revertPlan =
            update_Default(scanPlan,
                           new UpdateFunction()
                           {
                               @Override
                               public boolean usePValues() {
                                   return usingPValues();
                               }

                               @Override
                               public Row evaluate(Row original, QueryContext context)
                               {
                                   OverlayingRow updatedRow = new OverlayingRow(original);
                                   long i;
                                   if (usePValues()) {
                                       i = original.pvalue(0).getInt64();
                                   }
                                   else {
                                       i = getLong(original, 0);
                                   }
                                   updatedRow.overlay(0, i + 1000000);
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
                        updatePlan.run(queryContext);
                        revertPlan.run(queryContext);
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
