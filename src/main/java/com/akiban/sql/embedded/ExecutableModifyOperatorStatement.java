/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.embedded;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.ImmutableRow;
import com.akiban.qp.row.ProjectedRow;
import com.akiban.qp.row.Row;
import com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;
import com.akiban.sql.server.ServerSession;
import com.akiban.sql.server.ServerTransaction;
import com.akiban.util.ShareHolder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class ExecutableModifyOperatorStatement extends ExecutableOperatorStatement
{
    private boolean requireStepIsolation;

    private static final Logger logger = LoggerFactory.getLogger(ExecutableModifyOperatorStatement.class);

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
        Cursor cursor = null;
        RuntimeException runtimeException = null;
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
        }
        catch (RuntimeException ex) {
            runtimeException = ex;
        }
        finally {
            try {
                if (cursor != null) {
                    cursor.destroy();
                }
            }
            catch (RuntimeException ex) {
                if (runtimeException == null)
                    runtimeException = ex;
                else
                    logger.warn("Error cleaning up cursor with exception already pending", ex);
            }
            context.unlock(DXLFunction.UNSPECIFIED_DML_WRITE);
            if (runtimeException != null)
                throw runtimeException;
        }
        if (returningRows != null)
            returningRows.open(); // Done filling.
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

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.NOT_ALLOWED;
    }

    static class SpoolCursor implements Cursor {
        private List<ShareHolder<Row>> rows = new ArrayList<>();
        private Iterator<ShareHolder<Row>> iterator;
        private enum State { CLOSED, FILLING, EMPTYING, DESTROYED }
        private State state;
        
        public SpoolCursor() {
            state = State.FILLING;
        }

        public void add(Row row) {
            assert (state == State.FILLING);
            if (row instanceof ProjectedRow)
                // create a copy of this row, and hold it instead
                row = new ImmutableRow((ProjectedRow)row);
            ShareHolder<Row> holder = new ShareHolder<>();
            holder.hold(row);
            rows.add(holder);
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


