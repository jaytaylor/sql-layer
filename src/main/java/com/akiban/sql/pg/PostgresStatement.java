
package com.akiban.sql.pg;

import com.akiban.sql.optimizer.plan.CostEstimate;
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

    /** Should this statement be put into the statement cache? */
    public boolean putInCache();

    /** Get the estimated cost, if known and applicable. */
    public CostEstimate getCostEstimate();

}
