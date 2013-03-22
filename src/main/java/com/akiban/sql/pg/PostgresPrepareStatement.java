
package com.akiban.sql.pg;

import com.akiban.sql.parser.PrepareStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;

import java.util.List;
import java.io.IOException;

public class PostgresPrepareStatement extends PostgresBaseCursorStatement
{
    private String name;
    private String sql;
    private StatementNode stmt;

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        PrepareStatementNode prepare = (PrepareStatementNode)stmt;
        this.name = prepare.getName();
        this.stmt = prepare.getStatement();
        this.sql = sql.substring(this.stmt.getBeginOffset(), this.stmt.getEndOffset() + 1);
        return this;
    }
    
    @Override
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        server.prepareStatement(name, sql, stmt, null, null);
        {        
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString("PREPARE");
            messenger.sendMessage();
        }
        return 0;
    }
    
    @Override
    public boolean putInCache() {
        return false;
    }

}
