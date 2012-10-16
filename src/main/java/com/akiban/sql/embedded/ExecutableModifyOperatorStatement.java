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

package com.akiban.sql.embedded;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.ProjectedRow;
import com.akiban.qp.row.Row;
import com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;
import com.akiban.sql.server.ServerSession;
import com.akiban.sql.server.ServerTransaction;
import com.akiban.util.ShareHolder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class ExecutableModifyOperatorStatement extends ExecutableOperatorStatement
{
    private boolean requireStepIsolation;

    protected ExecutableModifyOperatorStatement(Operator resultOperator,
                                                JDBCResultSetMetaData resultSetMetaData, 
                                                JDBCParameterMetaData parameterMetaData,
                                                boolean requireStepIsolation) {
        super(resultOperator, resultSetMetaData, parameterMetaData);
        this.requireStepIsolation = requireStepIsolation;
    }
    
    @Override
    public ExecuteResults execute(EmbeddedQueryContext context) {
        int updateCount = 0;
        SpoolCursor  returningRows = null;
        if (resultSetMetaData != null)
            // If there are results, we need to read them all now to get the update
            // count right and have this all happen even if the caller
            // does not read all of the generated keys.
            returningRows = new SpoolCursor();
        context.lock(DXLFunction.UNSPECIFIED_DML_WRITE);
        ServerSession server = context.getServer();
        ServerTransaction localTransaction = server.beforeExecute(this);
        boolean success = false;
        Cursor cursor = null;
        try {
            cursor = API.cursor(resultOperator, context);
            cursor.open();
            Row row;
            while ((row = cursor.next()) != null) {
                updateCount++;
                if (returningRows != null) {
                    returningRows.add(row);
                }
            }
            success = true;
        }
        finally {
            try {
                if (cursor != null) {
                    cursor.destroy();
                }
            }
            catch (RuntimeException ex) {
            }
            server.afterExecute(this, localTransaction, success);
            context.unlock(DXLFunction.UNSPECIFIED_DML_WRITE);
        }
        return new ExecuteResults(updateCount, returningRows);
    }

    @Override
    public TransactionMode getTransactionMode() {
        if (requireStepIsolation)
            return TransactionMode.WRITE_STEP_ISOLATED;
        else
            return TransactionMode.WRITE;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    static class SpoolCursor implements Cursor {
        private List<ShareHolder<Row>> rows = new ArrayList<ShareHolder<Row>>();
        private Iterator<ShareHolder<Row>> iterator;
        private enum State { CLOSED, FILLING, EMPTYING, DESTROYED }
        private State state;
        
        public SpoolCursor() {
            state = State.FILLING;
        }

        public void add(Row row) {
            assert (state == State.FILLING);
            if (row instanceof ProjectedRow)
                ((ProjectedRow)row).freeze();
            ShareHolder<Row> holder = new ShareHolder<Row>();
            holder.hold(row);
        }

        @Override
        public void open() {
            CursorLifecycle.checkIdle(this);
            iterator = rows.iterator();
            state = State.EMPTYING;
        }        

        @Override
        public Row next() {
            CursorLifecycle.checkIdleOrActive(this);
            if (iterator.hasNext()) {
                ShareHolder<Row> holder = iterator.next();
                Row row = holder.get();
                holder.release();
                return row;
            }
            else {
                close();
                return null;
            }
        }

        @Override
        public void jump(Row row, com.akiban.server.api.dml.ColumnSelector columnSelector) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            CursorLifecycle.checkIdleOrActive(this);
            state = State.CLOSED;
        }
        
        @Override
        public void destroy()
        {
            close();
            state = State.DESTROYED;
        }

        @Override
        public boolean isIdle()
        {
            return ((state == State.CLOSED) || (state == State.FILLING));
        }

        @Override
        public boolean isActive()
        {
            return (state == State.EMPTYING);
        }

        @Override
        public boolean isDestroyed()
        {
            return (state == State.DESTROYED);
        }
    }

}


