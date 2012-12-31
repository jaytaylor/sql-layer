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
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.SetColumnSelector;
import com.akiban.server.error.InvalidOperationException;
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
        schema = new Schema(ais());
        parentRowType = schema.userTableRowType(userTable(parent));
        childRowType = schema.userTableRowType(userTable(child));
        parentPKIndexType = indexType(parent, "pid", "parent_instance");
        group = group(parent);
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
    private Group group;
}
