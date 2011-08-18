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

import com.akiban.server.error.MissingDDLParametersException;
import com.akiban.sql.parser.DDLStatementNode;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.ParameterNode;

import java.util.List;

/** DDL statements executed against AIS. */
public class PostgresDDLStatementGenerator extends PostgresBaseStatementGenerator
{
    public PostgresDDLStatementGenerator(PostgresServerSession server) {
    }

    @Override
    public PostgresStatement generate(PostgresServerSession server,
                                      StatementNode stmt, 
                                      List<ParameterNode> params,
                                      int[] paramTypes) {
        if (!(stmt instanceof DDLStatementNode))
            return null;
        if ((params != null) && !params.isEmpty())
            throw new MissingDDLParametersException ();
        return new PostgresDDLStatement((DDLStatementNode)stmt);
    }
}
