
package com.akiban.sql.pg;

import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.server.ServerSession;
import com.akiban.sql.server.ServerValueEncoder;

import java.util.List;
import java.io.IOException;

/** A Postgres server session. */
public interface PostgresServerSession extends ServerSession
{
    /** Return the protocol version in use. */
    public int getVersion();

    /** Return the messenger used to communicate with client. */
    public PostgresMessenger getMessenger();

    /** Return an encoder of values as bytes / strings. */
    public ServerValueEncoder getValueEncoder();

    public enum OutputFormat { TABLE, JSON, JSON_WITH_META_DATA };

    /** Get the output format. */
    public OutputFormat getOutputFormat();

    /** Prepare a statement and store by name. */
    public void prepareStatement(String name, 
                                 String sql, StatementNode stmt,
                                 List<ParameterNode> params, int[] paramTypes);

    /** Execute prepared statement. */
    public int executePreparedStatement(PostgresExecuteStatement estmt, int maxrows)
            throws IOException;

    /** Remove prepared statement with given name. */
    public void deallocatePreparedStatement(String name);

    /** Declare a named cursor. */
    public void declareStatement(String name, 
                                 String sql, StatementNode stmt);

    /** Fetch from named cursor. */
    public int fetchStatement(String name, int count) throws IOException;

    /** Remove declared cursor with given name. */
    public void closeBoundPortal(String name);
}
