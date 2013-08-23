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

package com.foundationdb.sql.aisddl;

import com.foundationdb.sql.ServerSessionITBase;
import com.foundationdb.sql.parser.DDLStatementNode;
import com.foundationdb.sql.parser.StatementNode;

public class AISDDLITBase extends ServerSessionITBase {
    protected void executeDDL(String sql) throws Exception {
        // Most of the state in this depends on the current AIS, which changes
        // as a result of this, so it's simplest to just make a new session
        // every time. Only views need all of the binder state, but
        // it's just as easy to make the parser this way.
        TestSession session = new TestSession();
        StatementNode stmt = session.getParser().parseStatement(sql);
        assert (stmt instanceof DDLStatementNode) : stmt;
        AISDDL.execute((DDLStatementNode)stmt, sql, new TestQueryContext(session));
    }

}
