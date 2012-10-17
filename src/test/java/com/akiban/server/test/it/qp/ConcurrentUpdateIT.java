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
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.UpdateFunction;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.row.OverlayingRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.util.SequencerConstants;
import com.akiban.server.util.ThreadSequencer;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static com.akiban.qp.operator.API.*;

@Ignore
public class ConcurrentUpdateIT extends OperatorITBase
{
    @Before
    public void before() throws InvalidOperationException
    {
        a = createTable(
            "schema", "a",
            "aid int not null primary key",
            "ax int");
        b = createTable(
            "schema", "b",
            "bid int not null primary key",
            "bx int");
        schema = new Schema(rowDefCache().ais());
        aRowType = schema.userTableRowType(userTable(a));
        bRowType = schema.userTableRowType(userTable(b));
        aGroup = group(a);
        bGroup = group(b);
        db = new NewRow[]{
            createNewRow(a, 1L, 101L),
            createNewRow(a, 2L, 102L),
            createNewRow(a, 3L, 103L),
            createNewRow(b, 4L, 204L),
            createNewRow(b, 5L, 205L),
            createNewRow(b, 6L, 206L),
        };
        adapter = persistitAdapter(schema);
        queryContext = queryContext(adapter);
    }

    @Before
    public void before_beginTransaction() throws PersistitException {
        // This test manages its own transactions
    }

    @After
    public void after_endTransaction() throws PersistitException {
        // This test manages its own transactions
    }

    @Test
    public void concurrentUpdate() throws Exception
    {
        ThreadSequencer.enableSequencer(true);
        ThreadSequencer.addSchedules(SequencerConstants.UPDATE_GET_CONTEXT_SCHEDULE);
        Transaction txn = adapter.transaction();
        txn.begin();
        use(db);
        txn.commit();
        txn.end();
        UpdateFunction updateAFunction = new UpdateFunction()
        {
            @Override
            public boolean usePValues() {
                return usingPValues();
            }

            @Override
            public boolean rowIsSelected(Row row)
            {
                return row.rowType().equals(aRowType);
            }

            @Override
            public Row evaluate(Row original, QueryContext context)
            {
                long ax;
                if (usePValues()) {
                    ax = original.pvalue(1).getInt64();
                }
                else {
                    ToObjectValueTarget target = new ToObjectValueTarget();
                    target.expectType(AkType.INT);
                    Object obj = Converters.convert(original.eval(1), target).lastConvertedValue();
                    ax = (Long) obj;
                }
                return new OverlayingRow(original).overlay(1, -ax);
            }
        };
        UpdateFunction updateBFunction = new UpdateFunction()
        {
            @Override
            public boolean usePValues() {
                return usingPValues();
            }

            @Override
            public boolean rowIsSelected(Row row)
            {
                return row.rowType().equals(bRowType);
            }

            @Override
            public Row evaluate(Row original, QueryContext context)
            {
                long bx;
                if (usePValues()) {
                    bx = original.pvalue(1).getInt64();
                }
                else {
                    ToObjectValueTarget target = new ToObjectValueTarget();
                    target.expectType(AkType.INT);
                    Object obj = Converters.convert(original.eval(1), target).lastConvertedValue();
                    bx = (Long) obj;
                }
                return new OverlayingRow(original).overlay(1, -bx);
            }
        };
        UpdatePlannable updateA = update_Default(groupScan_Default(aGroup), updateAFunction);
        UpdatePlannable updateB = update_Default(groupScan_Default(bGroup), updateBFunction);
/*
        TestThread threadA = new TestThread(aGroup, updateA);
        threadA.start();
        threadA.join();
*/
        TestThread threadA = new TestThread(aGroup, updateA);
        TestThread threadB = new TestThread(bGroup, updateB);
        threadA.start();
        threadB.start();
        threadA.join();
        threadB.join();
    }

    private class TestThread extends Thread
    {
        @Override
        public void run()
        {
            PersistitAdapter adapter = new PersistitAdapter(schema, store(), treeService(), createNewSession(), configService());
            QueryContext queryContext = queryContext(adapter);
            Transaction transaction = treeService().getTransaction(session());
            try {
                transaction.begin();
                plan.run(queryContext);
                dump(cursor(groupScan_Default(group), queryContext));
                transaction.commit();
                transaction.end();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public TestThread(Group group, UpdatePlannable plan)
        {
            setName(group.getName());
            this.group = group;
            this.plan = plan;
        }

        private Group  group;
        private UpdatePlannable plan;
    }

    private int a;
    private int b;
    private UserTableRowType aRowType;
    private UserTableRowType bRowType;
    private Group aGroup;
    private Group bGroup;
}
