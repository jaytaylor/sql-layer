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

package com.akiban.sql.pg;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;

import java.io.IOException;
import java.util.List;

import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An SQL modifying DML statement transformed into an operator tree
 * @see PostgresOperatorCompiler
 */
public class PostgresModifyOperatorStatement extends PostgresDMLStatement
{
    private String statementType;
    private Operator resultOperator;
    //private UpdatePlannable updater;
    private boolean requireStepIsolation;
    private boolean outputResult;

    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresBaseStatement: execute exclusive");
    private static final InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresBaseStatement: acquire exclusive lock");
    private static final Logger LOG = LoggerFactory.getLogger(PostgresModifyOperatorStatement.class);

    /*
    public PostgresModifyOperatorStatement(String statementType,
                                           UpdatePlannable resultOperator,
                                           PostgresType[] parameterTypes,
                                           boolean usesPValues,
                                           boolean requireStepIsolation) {
        super(parameterTypes, usesPValues);
        this.statementType = statementType;
        this.updater = resultOperator;
        this.requireStepIsolation = requireStepIsolation;
        outputResult = false;
    }
    */
    
    public PostgresModifyOperatorStatement (String statementType,
                                    Operator resultsOperator, 
                                    PostgresType[] parameterTypes,
                                    boolean usesPValues,
                                    boolean requireStepIsolation) {
        super (parameterTypes, usesPValues);
        this.statementType = statementType;
        this.resultOperator = resultsOperator;
        this.requireStepIsolation = requireStepIsolation;
        outputResult = false;
                
    }
    
    public PostgresModifyOperatorStatement (String statementType,
                                    Operator resultOperator,
                                     RowType resultRowType,
                                     List<String> columnNames,
                                     List<PostgresType> columnTypes,
                                     PostgresType[] parameterTypes,
                                     boolean usesPValues,
                                     boolean requireStepIsolation) {
        super(resultRowType, columnNames, columnTypes, parameterTypes, usesPValues);
        this.statementType = statementType;
        this.resultOperator = resultOperator;
        this.requireStepIsolation = requireStepIsolation;
        outputResult = true;
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

    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        boolean lockSuccess = false;
        int rowsModified = 0;
        if (resultOperator != null) {
            Cursor cursor = null;
            IOException exceptionDuringExecution = null;
            try {
                lock(context, DXLFunction.UNSPECIFIED_DML_WRITE);
                lockSuccess = true;
                cursor = API.cursor(resultOperator, context);
                cursor.open();
                PostgresOutputter<Row> outputter = null;
                if (outputResult) {
                    outputter = getRowOutputter(context);
                }
                Row row;
                while ((row = cursor.next()) != null) {
                    assert getResultRowType() == null || (row.rowType() == getResultRowType()) : row;
                    if (outputResult) { 
                        outputter.output(row, usesPValues());
                    }
                    rowsModified++;
                    if ((maxrows > 0) && (rowsModified >= maxrows))
                        outputResult = false;
                }
            }
            catch (IOException e) {
                exceptionDuringExecution = e;
            }
            finally {
                RuntimeException exceptionDuringCleanup = null;
                try {
                    if (cursor != null) {
                        cursor.destroy();
                    }
                }
                catch (RuntimeException e) {
                    exceptionDuringCleanup = e;
                    LOG.error("Caught exception while cleaning up cursor for {0}", resultOperator.describePlan());
                    LOG.error("Exception stack", e);
                }
                finally {
                    unlock(context, DXLFunction.UNSPECIFIED_DML_WRITE, lockSuccess);
                }
                if (exceptionDuringExecution != null) {
                    throw exceptionDuringExecution;
                } else if (exceptionDuringCleanup != null) {
                    throw exceptionDuringCleanup;
                }
            }
/*
        } else {
            final UpdateResult updateResult;
            try {
                lock(context, DXLFunction.UNSPECIFIED_DML_WRITE);
                lockSuccess = true;
                updateResult = updater.run(context);
                rowsModified = updateResult.rowsModified();
            } 
            finally {
                unlock(context, DXLFunction.UNSPECIFIED_DML_WRITE, lockSuccess);
            }
            LOG.debug("Statement: {}, result: {}", statementType, updateResult);
*/            
        }
        
        messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
        //TODO: Find a way to extract InsertNode#statementToString() or equivalent
        if (statementType.equals("INSERT")) {
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
    protected InOutTap acquireLockTap()
    {
        return ACQUIRE_LOCK_TAP;
    }
}
