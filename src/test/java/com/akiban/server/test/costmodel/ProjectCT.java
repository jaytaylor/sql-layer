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
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.TimeOperator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.test.ExpressionGenerators;
import org.junit.Test;

import java.util.Arrays;

import static com.akiban.qp.operator.API.*;

public class ProjectCT extends CostModelBase
{
    @Test
    public void run() throws Exception
    {
        createSchema();
        populateDB(ROWS);
        run(WARMUP_RUNS, false);
        run(MEASURED_RUNS, true);
    }

    private void createSchema() throws InvalidOperationException
    {
        String schemaName = schemaName();
        String tableName = newTableName();
        t = createTable(schemaName, tableName,
                        "id int not null",
                        "primary key(id)");
        schema = new Schema(rowDefCache().ais());
        tRowType = schema.userTableRowType(userTable(t));
        group = group(t);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    protected void populateDB(int rows)
    {
        for (int id = 0; id < rows; id++) {
            dml().writeRow(session(), createNewRow(t, id));
        }
    }

    private void run(int runs, boolean report)
    {
        Operator scan = groupScan_Default(group);
        TimeOperator timeScan = new TimeOperator(scan);
        Operator project = project_Default(timeScan, tRowType, Arrays.asList(ExpressionGenerators.literal(true)));
        long start = System.nanoTime();
        for (int r = 0; r < runs; r++) {
            Cursor cursor = cursor(project, queryContext);
            cursor.open();
            while (cursor.next() != null);
        }
        long stop = System.nanoTime();
        long projectNsec = stop - start - timeScan.elapsedNsec();
        if (report) {
            // Report the difference
            double averageUsecPerRow = projectNsec / (1000.0 * runs * ROWS);
            System.out.println(String.format("%s usec/row", averageUsecPerRow));
        }
    }

    private static final int ROWS = 100;
    private static final int WARMUP_RUNS = 20000;
    private static final int MEASURED_RUNS = 10000;

    private int t;
    private RowType tRowType;
    private Group group;
}
