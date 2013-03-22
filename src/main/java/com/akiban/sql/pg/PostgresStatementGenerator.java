
package com.akiban.sql.pg;

import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.ParameterNode;

import java.util.List;

/** Turn an SQL statement into something executable. */
public interface PostgresStatementGenerator extends PostgresStatementParser
{
    /** Return constructed, but potentially unusable, PostgresStatement for the given parsed
     *  statement, or <code>null</code> if this generator cannot handle it.
     *  statement.finishGenerating must be called before it is usable. */
    public PostgresStatement generateStub(PostgresServerSession server,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params, int[] paramTypes);
}
