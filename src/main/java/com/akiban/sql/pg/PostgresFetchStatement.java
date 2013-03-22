
package com.akiban.sql.pg;

import com.akiban.sql.parser.FetchStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;

import java.util.List;
import java.io.IOException;

public class PostgresFetchStatement extends PostgresBaseCursorStatement
{
    private String name;
    private int count;

    public String getName() {
        return name;
    }

    public void setParameters(PostgresBoundQueryContext context) {
        
    }

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        FetchStatementNode fetch = (FetchStatementNode)stmt;
        this.name = fetch.getName();
        this.count = fetch.getCount();
        return this;
    }
    
    @Override
    public void sendDescription(PostgresQueryContext context, boolean always) 
            throws IOException {
        // Execute will do it.
    }

    @Override
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        return server.fetchStatement(name, count);
    }
    
    @Override
    public boolean putInCache() {
        return true;
    }

}
