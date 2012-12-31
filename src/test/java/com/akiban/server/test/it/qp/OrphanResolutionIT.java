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

package com.akiban.server.test.it.qp;

import com.akiban.ais.model.Group;
import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.row.BindableRow;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.test.ExpressionGenerators;
import com.akiban.server.types.AkType;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static com.akiban.qp.operator.API.*;

public class OrphanResolutionIT extends OperatorITBase
{
    @Override
    protected void setupCreateSchema()
    {
        parent = createTable(
            "schema", "parent",
            "pid int not null",
            "px int",
            "primary key(pid)");
        child = createTable(
            "schema", "child",
            "pid int",
            "cx int",
            "grouping foreign key(pid) references parent(pid)");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        schema = new Schema(ais());
        parentRowType = schema.userTableRowType(userTable(parent));
        childRowType = schema.userTableRowType(userTable(child));
        group = group(parent);
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
        db = new NewRow[] {
            createNewRow(child, 1L, 100L),
            createNewRow(child, 1L, 101L),
        };
        use(db);
    }

    // Inspired by bug 1020342.

    @Test
    public void test()
    {
        UpdatePlannable insertPlan =
            insert_Default(
                valuesScan_Default(
                    Arrays.asList(parentRow(1, 10)), parentRowType));
        insertPlan.run(queryContext);
        // Execution of insertPlan used to hang before 1020342 was fixed.
        RowBase[] expected = new RowBase[] {
            row(parentRowType, 1L, 10L),
            // Last column of child rows is generated PK value
            row(childRowType, 1L, 100L, 1L),
            row(childRowType, 1L, 101L, 2L),
        };
        compareRows(expected, cursor(groupScan_Default(group), queryContext));
    }

    private BindableRow parentRow(int pid, int px)
    {
        return BindableRow.of(parentRowType, Arrays.asList(ExpressionGenerators.literal(pid, AkType.INT),
                                                           ExpressionGenerators.literal(px, AkType.INT)), null);
    }

    private int parent;
    private int child;
    private UserTableRowType parentRowType;
    private UserTableRowType childRowType;
    private Group group;
}
