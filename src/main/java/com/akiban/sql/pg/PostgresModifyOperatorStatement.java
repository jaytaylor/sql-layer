/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import com.akiban.qp.exec.UpdatePlannable;
import com.akiban.qp.exec.UpdateResult;

import java.util.List;
import java.io.IOException;

import com.akiban.server.service.dxl.DXLReadWriteLockHook;
import com.akiban.server.service.session.Session;
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

    private static final Tap.InOutTap EXECUTE_TAP = Tap.createTimer("PostgresBaseStatement: execute exclusive");
    private static final Tap.InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresBaseStatement: acquire exclusive lock");
    private static final Logger LOG = LoggerFactory.getLogger(PostgresModifyOperatorStatement.class);
        
    public PostgresModifyOperatorStatement(String statementType,
                                           UpdatePlannable resultOperator,
                                           PostgresType[] parameterTypes) {
        super(parameterTypes);
        this.statementType = statementType;
        this.resultOperator = resultOperator;
    }
    
    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.WRITE;
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
    protected Tap.InOutTap executeTap()
    {
        return EXECUTE_TAP;
    }

    @Override
    protected Tap.InOutTap acquireLockTap()
    {
        return ACQUIRE_LOCK_TAP;
    }
}
