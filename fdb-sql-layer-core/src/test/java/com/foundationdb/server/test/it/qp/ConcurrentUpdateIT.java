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

package com.foundationdb.server.test.it.qp;

import com.foundationdb.ais.model.Group;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.operator.UpdateFunction;
import com.foundationdb.qp.row.OverlayingRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.rowtype.TableRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.util.SequencerConstants;
import com.foundationdb.server.util.ThreadSequencer;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.foundationdb.qp.operator.API.*;
import static org.junit.Assert.assertEquals;

@Ignore
public class ConcurrentUpdateIT extends OperatorITBase
{
    private final AtomicBoolean hadAnyFailure = new AtomicBoolean();

    @Override
    protected void setupCreateSchema()
    {
        a = createTable(
            "schema", "a",
            "aid int not null primary key",
            "ax int");
        b = createTable(
            "schema", "b",
            "bid int not null primary key",
            "bx int");
    }

    @Override
    protected void setupPostCreateSchema()
    {
        aRowType = schema.tableRowType(table(a));
        bRowType = schema.tableRowType(table(b));
        aGroup = group(a);
        bGroup = group(b);
        db = new Row[]{
            row(a, 1L, 101L),
            row(a, 2L, 102L),
            row(a, 3L, 103L),
            row(b, 4L, 204L),
            row(b, 5L, 205L),
            row(b, 6L, 206L),
        };
        queryContext = queryContext(adapter);
        queryBindings = queryContext.createBindings();
    }

    @Override
    public boolean doAutoTransaction() {
        return false;
    }

    @Before
    public void resetFailure() {
        hadAnyFailure.set(false);
    }

    @Test
    public void concurrentUpdate() throws Exception
    {
        ThreadSequencer.enableSequencer(true);
        ThreadSequencer.addSchedules(SequencerConstants.UPDATE_GET_CONTEXT_SCHEDULE);
        txnService().beginTransaction(session());
        try {
            use(db);
        } finally {
            txnService().commitTransaction(session());
        }
        UpdateFunction updateAFunction = new UpdateFunction()
        {
            @Override
            public Row evaluate(Row original, QueryContext context, QueryBindings bindings)
            {
                long ax = original.value(1).getInt64();
                return new OverlayingRow(original).overlay(1, -ax);
            }
        };
        UpdateFunction updateBFunction = new UpdateFunction()
        {
            @Override
            public Row evaluate(Row original, QueryContext context, QueryBindings bindings)
            {
                long bx = original.value(1).getInt64();
                return new OverlayingRow(original).overlay(1, -bx);
            }
        };

        Operator updateA = update_Returning(groupScan_Default(aGroup), updateAFunction);
        Operator updateB = update_Returning(groupScan_Default(bGroup), updateBFunction);

        TestThread threadA = new TestThread(aGroup, updateA);
        TestThread threadB = new TestThread(bGroup, updateB);
        threadA.start();
        threadB.start();
        threadA.join();
        threadB.join();

        assertEquals("Had any failure", false, hadAnyFailure.get());
    }

    private class TestThread extends Thread
    {
        @Override
        public void run()
        {
            try(Session session = createNewSession()) {
                StoreAdapter adapter = newStoreAdapter(session);
                QueryContext queryContext = queryContext(adapter);
                try(TransactionService.CloseableTransaction txn = txnService().beginCloseableTransaction(session)) {
                    runPlan(queryContext, queryBindings, plan);
                    dump(cursor(groupScan_Default(group), queryContext, queryBindings));
                    txn.commit();
                } catch (Throwable e) {
                    hadAnyFailure.set(true);
                    e.printStackTrace();
                }
            }
        }

        public TestThread(Group group, Operator plan)
        {
            setName(group.getName().toString());
            this.group = group;
            this.plan = plan;
        }

        private Group  group;
        private Operator plan;
    }

    private int a;
    private int b;
    private TableRowType aRowType;
    private TableRowType bRowType;
    private Group aGroup;
    private Group bGroup;
}
