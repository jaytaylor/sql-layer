
package com.akiban.sql.pg;

/** Turn an SQL statement into something executable. */
public interface PostgresStatementParser
{
    /** Return executable form of the given statement or
     * <code>null</code> if this generator cannot handle it. */
    public PostgresStatement parse(PostgresServerSession server,
                                   String sql, int[] paramTypes);

    /** Notification that an attribute or schema has changed. */
    public void sessionChanged(PostgresServerSession server);
}
