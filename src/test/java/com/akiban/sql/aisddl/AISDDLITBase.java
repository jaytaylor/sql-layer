
package com.akiban.sql.aisddl;

import com.akiban.sql.ServerSessionITBase;
import com.akiban.sql.parser.DDLStatementNode;
import com.akiban.sql.parser.StatementNode;

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
