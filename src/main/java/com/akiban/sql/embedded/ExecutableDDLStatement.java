
package com.akiban.sql.embedded;

import com.akiban.sql.aisddl.AISDDL;
import com.akiban.sql.parser.DDLStatementNode;
import static com.akiban.server.service.dxl.DXLFunctionsHook.DXLFunction;

class ExecutableDDLStatement extends ExecutableStatement
{
    private DDLStatementNode ddl;
    private String sql;

    protected ExecutableDDLStatement(DDLStatementNode ddl, String sql) {
        this.ddl = ddl;
        this.sql = sql;
    }

    @Override
    public ExecuteResults execute(EmbeddedQueryContext context) {
        context.lock(DXLFunction.UNSPECIFIED_DDL_WRITE);
        try {
            AISDDL.execute(ddl, sql, context);
        }
        finally {
            context.unlock(DXLFunction.UNSPECIFIED_DDL_WRITE);
        }
        return new ExecuteResults();
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

}
