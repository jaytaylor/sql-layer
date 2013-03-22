
package com.akiban.sql.pg;

import com.akiban.sql.parser.CloseStatementNode;
import com.akiban.sql.parser.DeallocateStatementNode;
import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;

import java.util.List;
import java.io.IOException;

public class PostgresCloseCursorStatement extends PostgresBaseCursorStatement
{
    private String name;
    private boolean preparedStatement;

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        if (stmt.getNodeType() == NodeTypes.DEALLOCATE_STATEMENT_NODE) {
            name = ((DeallocateStatementNode)stmt).getName();
            preparedStatement = true;
        }
        else {
            name = ((CloseStatementNode)stmt).getName();
            preparedStatement = false;
        }
        return this;
    }
    
    @Override
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        if (preparedStatement)
            server.deallocatePreparedStatement(name);
        else
            server.closeBoundPortal(name);
        {        
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString(preparedStatement ? "DEALLOCATE" : "CLOSE");
            messenger.sendMessage();
        }
        return 0;
    }
    
    @Override
    public boolean putInCache() {
        return true;
    }

}
