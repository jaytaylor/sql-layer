/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
