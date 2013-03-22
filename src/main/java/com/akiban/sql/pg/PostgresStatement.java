/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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
