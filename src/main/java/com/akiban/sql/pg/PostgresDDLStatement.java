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

package com.akiban.sql.pg;

import com.akiban.sql.aisddl.AISDDL;
import com.akiban.sql.parser.DDLStatementNode;

import com.akiban.qp.operator.QueryBindings;

import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;

/** SQL DDL statements. */
public class PostgresDDLStatement extends PostgresBaseStatement
{
    private static final Logger logger = LoggerFactory.getLogger(PostgresDDLStatement.class);
    private static final InOutTap EXECUTE_TAP = Tap.createTimer("PostgresDDLStatement: execute shared");
    private static final InOutTap ACQUIRE_LOCK_TAP = Tap.createTimer("PostgresDDLStatement: acquire shared lock");

    private DDLStatementNode ddl;
    private String sql;

    public PostgresDDLStatement(DDLStatementNode ddl, String sql) {
        this.sql = sql;
        this.ddl = ddl;
    }

    @Override
    public PostgresType[] getParameterTypes() {
        return null;
    }

    @Override
    public void sendDescription(PostgresQueryContext context,
                                boolean always, boolean params)
            throws IOException {
        if (always) {
            PostgresServerSession server = context.getServer();
            PostgresMessenger messenger = server.getMessenger();
            if (params) {
                messenger.beginMessage(PostgresMessages.PARAMETER_DESCRIPTION_TYPE.code());
                messenger.writeShort(0);
                messenger.sendMessage();
            }
            messenger.beginMessage(PostgresMessages.NO_DATA_TYPE.code());
            messenger.sendMessage();
        }
    }

    @Override
    public TransactionMode getTransactionMode() {
        return TransactionMode.NONE;
    }

    @Override
    public TransactionAbortedMode getTransactionAbortedMode() {
        return TransactionAbortedMode.NOT_ALLOWED;
    }

    @Override
    public AISGenerationMode getAISGenerationMode() {
        return AISGenerationMode.ALLOWED;
    }

    @Override
    public boolean putInCache() {
        return false;
    }

    @Override
    public int execute(PostgresQueryContext context, QueryBindings bindings, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        PostgresMessenger messenger = server.getMessenger();
        boolean lockSuccess = false;
        try {
            lock(context, DXLFunction.UNSPECIFIED_DDL_WRITE);
            lockSuccess = true;
            AISDDL.execute(ddl, sql, context);
        }
        finally {
            unlock(context, DXLFunction.UNSPECIFIED_DDL_WRITE, lockSuccess);
        }
        {        
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString(ddl.statementToString());
            messenger.sendMessage();
        }
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
