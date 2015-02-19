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

package com.foundationdb.sql.embedded;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.ExecutionBase;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.row.ImmutableRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class ExecutableModifyOperatorStatement extends ExecutableOperatorStatement
{
    private static final Logger logger = LoggerFactory.getLogger(ExecutableModifyOperatorStatement.class);

    protected ExecutableModifyOperatorStatement(Operator resultOperator,
                                                long aisGeneration,
                                                JDBCResultSetMetaData resultSetMetaData, 
                                                JDBCParameterMetaData parameterMetaData) {
        super(resultOperator, aisGeneration, resultSetMetaData, parameterMetaData);
    }
    
    @Override
    public StatementTypes getStatementType() {
        return StatementTypes.DML_STMT;
    }

    @Override
    public ExecuteResults execute(EmbeddedQueryContext context, QueryBindings bindings) {
        int updateCount = 0;
        SpoolCursor returningRows = null;
        if (resultSetMetaData != null)
            // If there are results, we need to read them all now to get the update
            // count right and have this all happen even if the caller
            // does not read all of the generated keys.
            returningRows = new SpoolCursor();
        Cursor cursor = null;
        RuntimeException runtimeException = null;
        try {
            cursor = API.cursor(resultOperator, context, bindings);
            cursor.openTopLevel();
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
                    cursor.closeTopLevel();
                }
            }
            catch (RuntimeException ex) {
                if (runtimeException == null)
                    runtimeException = ex;
                else
                    logger.warn("Error cleaning up cursor with exception already pending", ex);
            }
            if (runtimeException != null)
                throw runtimeException;
        }
        if (returningRows != null)
            returningRows.open(); // Done filling.
        return new ExecuteResults(updateCount, returningRows);
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.WRITE;
    }

    static class SpoolCursor  extends RowCursorImpl  {
        private List<Row> rows = new ArrayList<>();
        private Iterator<Row> iterator;
        private enum State { FILLING, EMPTYING}
        private State spoolState;
        
        public SpoolCursor() {
            spoolState = State.FILLING;
        }

        public void add(Row row) {
            assert (spoolState == State.FILLING);
            if (row.isBindingsSensitive())
                // create a copy of this row, and hold it instead
                row = ImmutableRow.buildImmutableRow(row);
            rows.add(row);
        }

        @Override
        public void open() {
            super.open();
            iterator = rows.iterator();
            spoolState = State.EMPTYING;
        }        

        @Override
        public Row next() {
            if (ExecutionBase.CURSOR_LIFECYCLE_ENABLED) {
                CursorLifecycle.checkIdleOrActive(this);
            }
            assert (spoolState == State.EMPTYING);
            if (iterator.hasNext()) {
                Row row = iterator.next();
                return row;
            }
            else {
                setIdle();
                return null;
            }
        }
    }
}


