package com.akiban.sql.pg;

import java.util.List;

import com.akiban.sql.parser.AlterServerNode;
import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;

public class PostgresServerStatementGenerator extends
        PostgresBaseStatementGenerator {

    public PostgresServerStatementGenerator (PostgresServerSession server) {
    }
    
    @Override
    public PostgresStatement generateStub(PostgresServerSession server,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params, int[] paramTypes) {

        if (stmt.getNodeType() != NodeTypes.ALTER_SERVER_NODE) 
            return null;
        return new PostgresServerStatement ((AlterServerNode)stmt);
    }
}
