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
import com.akiban.sql.parser.ParameterNode;

import com.akiban.sql.StandardException;

import java.util.List;

/** Turn an SQL statement into something executable. */
public interface PostgresStatementGenerator extends PostgresStatementParser
{

    /** Return executable form of the given parsed statement or
     * <code>null</code> if this generator cannot handle it. */
    public PostgresStatement generate(PostgresServerSession server,
                                      StatementNode stmt, 
                                      List<ParameterNode> params, int[] paramTypes) 
            throws StandardException;

}
