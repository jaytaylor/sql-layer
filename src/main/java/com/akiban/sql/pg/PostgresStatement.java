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
import com.akiban.sql.server.ServerStatement;

import java.io.IOException;
import java.util.List;

/**
 * An SQL statement compiled for use with Postgres server.
 * @see PostgresStatementGenerator
 */
public interface PostgresStatement extends ServerStatement
{
    /** Get the types of any parameters. */
    public PostgresType[] getParameterTypes();

    /** Send a description message. If <code>always</code>, do so even
     * if no result set. */
    public void sendDescription(PostgresQueryContext context, boolean always) throws IOException;

    /** Execute statement and output results. Return number of rows processed. */
    public int execute(PostgresQueryContext context, int maxrows) throws IOException;

    /** Whether or not the generation has been set */
    public boolean hasAISGeneration();

    /** Set generation this statement was created under */
    public void setAISGeneration(long aisGeneration);

    /** Get generation this statement was created under */
    public long getAISGeneration();

    /** Finish constructing this statement. Returning a new instance is allowed. */
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes);
}
