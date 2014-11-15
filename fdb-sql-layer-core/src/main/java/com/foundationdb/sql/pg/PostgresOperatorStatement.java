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

package com.foundationdb.sql.pg;

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.sql.optimizer.plan.CostEstimate;
import com.foundationdb.qp.operator.*;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;

import java.util.*;
import java.io.IOException;

/**
 * An SQL SELECT transformed into an operator tree
 * @see PostgresOperatorCompiler
 */
public class PostgresOperatorStatement extends PostgresBaseOperatorStatement 
                                       implements PostgresCursorGenerator<Cursor>
{
    private Operator resultOperator;
    private CostEstimate costEstimate;

    private static final Logger logger = LoggerFactory.getLogger(PostgresOperatorStatement.class);
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresOperatorStatement: execute shared");

    public PostgresOperatorStatement(PostgresOperatorCompiler compiler) {
        super(compiler);
    }

    public void init(Operator resultOperator,
                     RowType resultRowType,
                     List<String> columnNames,
                     List<PostgresType> columnTypes,
                     List<Column> aisColumns,
                     PostgresType[] parameterTypes,
                     CostEstimate costEstimate) {
        super.init(resultRowType, columnNames, columnTypes, aisColumns, parameterTypes);
        this.resultOperator = resultOperator;
        this.costEstimate = costEstimate;
    }
    
    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.READ;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.NOT_ALLOWED;
    }

    @Override
    public boolean canSuspend(PostgresServerSession server) {
        return server.isTransactionActive();
    }

    @Override
    public Cursor openCursor(PostgresQueryContext context, QueryBindings bindings) {
        Cursor cursor = API.cursor(resultOperator, context, bindings);
        cursor.openTopLevel();
        return cursor;
    }

    public void closeCursor(Cursor cursor) {
        if (cursor != null) {
            cursor.closeTopLevel();
        }
    }
    
    @Override
    public int execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        int nrows = 0;
        Cursor cursor = null;
        IOException exceptionDuringExecution = null;
        RuntimeException runtimeExDuringExecution = null;
        boolean suspended = false;
        try {
            preExecute(context, DXLFunction.UNSPECIFIED_DML_READ);
            context.initStore(getSchema());
            cursor = context.startCursor(this, bindings);
            PostgresOutputter<Row> outputter = getRowOutputter(context);
            outputter.beforeData();
            if (cursor != null) {
                Row row;
                while ((row = cursor.next()) != null) {
                    assert (getResultRowType() == null) || (row.rowType() == getResultRowType()) : row;
                    outputter.output(row);
                    nrows++;
                    if ((maxrows > 0) && (nrows >= maxrows)) {
                        suspended = true;
                        break;
                    }
                }
            }
            outputter.afterData();
        }
        catch (IOException e) {
            exceptionDuringExecution = e;
        } catch (InvalidOperationException e) {
            e.getCode().logAtImportance(logger, 
                    "Caught unexpected InvalidOperationException during execution: {}", 
                    e.getLocalizedMessage());
            runtimeExDuringExecution = e;
        } catch (RuntimeException e) {
            logger.error("Caught unexpected runtime exception during execution {}", e.getLocalizedMessage());
            runtimeExDuringExecution = e;
        }
        finally {
            RuntimeException exceptionDuringCleanup = null;
            try {
                suspended = context.finishCursor(this, cursor, nrows, suspended);
            } catch (InvalidOperationException e) {
                e.getCode().logAtImportance(logger, 
                        "Caught unexpected InvalidOperationException during cleanup: {}", 
                        e.getLocalizedMessage());
                exceptionDuringCleanup = e;
            } catch (RuntimeException e) {
                exceptionDuringCleanup = e;
                logger.error("Caught exception while cleaning up cursor for {} : {}", resultOperator.describePlan(), e.getLocalizedMessage());
            }
            finally {
                postExecute(context, DXLFunction.UNSPECIFIED_DML_READ);
            }
            if (exceptionDuringExecution != null) {
                throw exceptionDuringExecution;
            } else if (runtimeExDuringExecution != null) {
                throw runtimeExDuringExecution;
            } else if (exceptionDuringCleanup != null) {
                throw exceptionDuringCleanup;
            }
        }
        if (suspended) {
            messenger.beginMessage(PostgresMessages.PORTAL_SUSPENDED_TYPE.code());
            messenger.sendMessage();
        }
        else {
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString("SELECT " + nrows);
            messenger.sendMessage();
        }
        return nrows;
    }

    @Override
    protected InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    public CostEstimate getCostEstimate() {
        return costEstimate;
    }

}
