
package com.akiban.sql.pg;

import com.akiban.server.error.MissingDDLParametersException;
import com.akiban.sql.parser.DDLStatementNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.ParameterNode;

import java.util.List;

/** DDL statements executed against AIS. */
public class PostgresDDLStatementGenerator extends PostgresBaseStatementGenerator
{
    public PostgresDDLStatementGenerator(PostgresServerSession server) {
    }

    @Override
    public PostgresStatement generateStub(PostgresServerSession server,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params,
                                          int[] paramTypes) {
        if (!(stmt instanceof DDLStatementNode))
            return null;
        if ((params != null) && !params.isEmpty())
            throw new MissingDDLParametersException ();
        return new PostgresDDLStatement((DDLStatementNode)stmt, sql);
    }
}
