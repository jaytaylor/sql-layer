/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.sql.pg;

import com.foundationdb.sql.parser.ParameterNode;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.server.ServerSession;
import com.foundationdb.sql.server.ServerValueEncoder;

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
