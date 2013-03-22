
package com.akiban.sql.pg;

import com.akiban.sql.parser.DeclareStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;

import java.util.List;
import java.io.IOException;

public class PostgresDeclareStatement extends PostgresBaseCursorStatement
{
    private String name;
    private String sql;
    private StatementNode stmt;

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        DeclareStatementNode declare = (DeclareStatementNode)stmt;
        this.name = declare.getName();
        this.stmt = declare.getStatement();
        this.sql = sql.substring(this.stmt.getBeginOffset(), this.stmt.getEndOffset() + 1);
        return this;
    }
    
    @Override
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        server.declareStatement(name, sql, stmt);
        {        
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString("DECLARE");
            messenger.sendMessage();
        }
        return 0;
    }
    
    @Override
    public boolean putInCache() {
        return false;
    }

}
