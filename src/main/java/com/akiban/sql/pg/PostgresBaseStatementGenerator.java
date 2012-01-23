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

import com.akiban.server.error.SQLParseException;
import com.akiban.server.error.SQLParserInternalException;
import com.akiban.sql.StandardException;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.SQLParserException;

public abstract class PostgresBaseStatementGenerator 
                implements PostgresStatementGenerator
{

    @Override
    public PostgresStatement parse(PostgresServerSession server,
                                   String sql, int[] paramTypes) {
        // This very inefficient reparsing by every generator is actually avoided.
        SQLParser parser = server.getParser();
        try {
            return generate(server, parser.parseStatement(sql), 
                            parser.getParameterList(), paramTypes);
        }
        catch (SQLParserException ex) {
            throw new SQLParseException(ex);
        }
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
    }

    @Override
    public void sessionChanged(PostgresServerSession server) {
    }

}
