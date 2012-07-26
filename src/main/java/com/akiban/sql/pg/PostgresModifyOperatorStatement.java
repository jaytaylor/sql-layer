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

import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;

import java.io.IOException;

import com.akiban.server.service.session.Session;
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction.*;

/**
 * An SQL modifying DML statement transformed into an operator tree
 * @see PostgresOperatorCompiler
 */
public class PostgresModifyOperatorStatement extends PostgresBaseStatement
{
    private String statementType;
    private UpdatePlannable resultOperator;

    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresBaseStatement: execute exclusive");
    private static final InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresBaseStatement: acquire exclusive lock");
    private static final Logger LOG = LoggerFactory.getLogger(PostgresModifyOperatorStatement.class);
        
    public PostgresModifyOperatorStatement(String statementType,
                                           UpdatePlannable resultOperator,
                                           PostgresType[] parameterTypes,
                                           boolean usesPValues) {
        super(parameterTypes, usesPValues);
        this.statementType = statementType;
        this.resultOperator = resultOperator;
    }
    
    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.WRITE;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        Session session = server.getSession();
        final UpdateResult updateResult;
        try {
            lock(session, UNSPECIFIED_DML_WRITE);
            updateResult = resultOperator.run(context);
        } finally {
            unlock(session, UNSPECIFIED_DML_WRITE);
        }

        LOG.debug("Statement: {}, result: {}", statementType, updateResult);
        
        messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
        //TODO: Find a way to extract InsertNode#statementToString() or equivalent
        if (statementType.equals("INSERT")) {
            messenger.writeString(statementType + " 0 " + updateResult.rowsModified());
        } else {
            messenger.writeString(statementType + " " + updateResult.rowsModified());
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
