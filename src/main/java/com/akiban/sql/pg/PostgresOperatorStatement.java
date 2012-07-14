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

import com.akiban.server.service.session.Session;
import com.akiban.qp.operator.*;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.sql.server.ServerStatement;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction.*;

import java.util.*;
import java.io.IOException;

/**
 * An SQL SELECT transformed into an operator tree
 * @see PostgresOperatorCompiler
 */
public class PostgresOperatorStatement extends PostgresBaseStatement
{
    private Operator resultOperator;
    private RowType resultRowType;

    private static final Logger logger = LoggerFactory.getLogger(PostgresOperatorStatement.class);
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresOperatorStatement: execute shared");
    private static final InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresOperatorStatement: acquire shared lock");

    public PostgresOperatorStatement(Operator resultOperator,
                                     RowType resultRowType,
                                     List<String> columnNames,
                                     List<PostgresType> columnTypes,
                                     PostgresType[] parameterTypes) {
        super(columnNames, columnTypes, parameterTypes);
        this.resultOperator = resultOperator;
        this.resultRowType = resultRowType;
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
    public int execute(PostgresQueryContext context, int maxrows, boolean usePVals) throws IOException {
        PostgresServerSession server = context.getServer();
        Session session = server.getSession();
        int nrows = 0;
        Cursor cursor = null;
        IOException exceptionDuringExecution = null;
        try {
            lock(session, UNSPECIFIED_DML_READ);
            cursor = API.cursor(resultOperator, context);
            cursor.open();
            PostgresOutputter<Row> outputter = getRowOutputter(context);
            Row row;
            while ((row = cursor.next()) != null) {
                assert resultRowType == null || (row.rowType() == resultRowType) : row;
                outputter.output(row, usePVals);
                nrows++;
                if ((maxrows > 0) && (nrows >= maxrows))
                    break;
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
                logger.error("Caught exception while cleaning up cursor for {0}", resultOperator.describePlan());
                logger.error("Exception stack", e);
            }
            finally {
                unlock(session, UNSPECIFIED_DML_READ);
            }
            if (exceptionDuringExecution != null) {
                throw exceptionDuringExecution;
            } else if (exceptionDuringCleanup != null) {
                throw exceptionDuringCleanup;
            }
        }
        {
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString("SELECT " + nrows);
            messenger.sendMessage();
        }
        return nrows;
    }

    protected PostgresOutputter<Row> getRowOutputter(PostgresQueryContext context) {
        return new PostgresRowOutputter(context, this);
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
