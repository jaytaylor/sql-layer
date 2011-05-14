/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.pg;

import com.akiban.sql.parser.StatementNode;

import com.akiban.sql.StandardException;

/** Turn an SQL statement into something executable. */
public interface PostgresStatementParser
{
    /** Return executable form of the given statement or
     * <code>null</code> if this generator cannot handle it. */
    public PostgresStatement parse(PostgresServerSession server,
                                   String sql, int[] paramTypes) 
            throws StandardException;

    /** Notification that an attribute or schema has changed. */
    public void sessionChanged(PostgresServerSession server);
}
