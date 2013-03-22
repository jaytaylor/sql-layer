
package com.akiban.sql.pg;

import com.akiban.sql.aisddl.AISDDL;
import com.akiban.sql.parser.DDLStatementNode;

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
    public void sendDescription(PostgresQueryContext context, boolean always) 
            throws IOException {
        if (always) {
            PostgresServerSession server = context.getServer();
            PostgresMessenger messenger = server.getMessenger();
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
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
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
