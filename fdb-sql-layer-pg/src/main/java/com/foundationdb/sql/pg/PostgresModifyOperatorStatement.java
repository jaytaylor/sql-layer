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
import com.foundationdb.sql.optimizer.plan.CostEstimate;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.service.dxl.DXLFunctionsHook.DXLFunction;

import java.io.IOException;
import java.util.List;

import com.foundationdb.util.tap.InOutTap;
import com.foundationdb.util.tap.Tap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An SQL modifying DML statement transformed into an operator tree
 * @see PostgresOperatorCompiler
 */
public class PostgresModifyOperatorStatement extends PostgresBaseOperatorStatement
                                             implements PostgresCursorGenerator<Cursor>
{
    private String statementType;
    private Operator resultOperator;
    private boolean outputResult;
    private boolean putInCache = true;
    private CostEstimate costEstimate;

    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresBaseStatement: execute exclusive");
    private static final Logger LOG = LoggerFactory.getLogger(PostgresModifyOperatorStatement.class);

    public PostgresModifyOperatorStatement(PostgresOperatorCompiler compiler) {
        super(compiler);
    }

    public void init(String statementType,
                     Operator resultsOperator,
                     PostgresType[] parameterTypes,
                     CostEstimate costEstimate,
                     boolean putInCache) {
        super.init(parameterTypes);
        this.statementType = statementType;
        this.resultOperator = resultsOperator;
        this.costEstimate = costEstimate;
        outputResult = false;
        this.putInCache = putInCache;
    }
    
    public void init(String statementType,
                     Operator resultOperator,
                     RowType resultRowType,
                     List<String> columnNames,
                     List<PostgresType> columnTypes,
                     List<Column> aisColumns,
                     PostgresType[] parameterTypes,
                     CostEstimate costEstimate,
                     boolean putInCache) {
        super.init(resultRowType, columnNames, columnTypes, aisColumns, parameterTypes);
        this.statementType = statementType;
        this.resultOperator = resultOperator;
        this.costEstimate = costEstimate;
        outputResult = true;
        this.putInCache = putInCache;
    }

    public boolean isInsert() {
        return "INSERT".equals(statementType);
    }

    @Override
    public TransactionMode getTransactionMode() {
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

    @Override
    public boolean putInCache() {
        return putInCache;
    }

    @Override
    public boolean canSuspend(PostgresServerSession server) {
        return false;           // See below.
    }

    @Override
    public Cursor openCursor(PostgresQueryContext context, QueryBindings bindings) {
        Cursor cursor = API.cursor(resultOperator, context, bindings);
        cursor.openTopLevel();
        return cursor;
    }

    @Override
    public void closeCursor(Cursor cursor) {
        if (cursor != null) {
            cursor.closeTopLevel();
        }
    }
    
    @Override
    public int execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        int rowsModified = 0;
        if (resultOperator != null) {
            Cursor cursor = null;
            IOException exceptionDuringExecution = null;
            try {
                preExecute(context, DXLFunction.UNSPECIFIED_DML_WRITE);
                cursor = openCursor(context, bindings);
                PostgresOutputter<Row> outputter = null;
                if (outputResult) {
                    outputter = getRowOutputter(context);
                    outputter.beforeData();
                }
                Row row;
                while ((row = cursor.next()) != null) {
                    assert getResultRowType() == null || (row.rowType() == getResultRowType()) : row;
                    if (outputResult) { 
                        outputter.output(row);
                    }
                    rowsModified++;
                    if ((maxrows > 0) && (rowsModified >= maxrows))
                        // Note: do not allow suspending, since the
                        // actual modifying and not just the generated
                        // key output would be incomplete.
                        outputResult = false;
                }
            }
            catch (IOException e) {
                exceptionDuringExecution = e;
            }
            finally {
                RuntimeException exceptionDuringCleanup = null;
                try {
                    closeCursor(cursor);
                }
                catch (RuntimeException e) {
                    exceptionDuringCleanup = e;
                    LOG.error("Caught exception while cleaning up cursor for {0}", resultOperator.describePlan());
                    LOG.error("Exception stack", e);
                }
                finally {
                    postExecute(context, DXLFunction.UNSPECIFIED_DML_WRITE);
                }
                if (exceptionDuringExecution != null) {
                    throw exceptionDuringExecution;
                } else if (exceptionDuringCleanup != null) {
                    throw exceptionDuringCleanup;
                }
            }
        }
        
        messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
        //TODO: Find a way to extract InsertNode#statementToString() or equivalent
        if (isInsert()) {
            messenger.writeString(statementType + " 0 " + rowsModified);
        } else {
            messenger.writeString(statementType + " " + rowsModified);
        }
        messenger.sendMessage();
        return 0;
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
