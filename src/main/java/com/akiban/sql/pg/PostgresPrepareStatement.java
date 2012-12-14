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

import com.akiban.sql.parser.PrepareStatementNode;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;

import java.util.List;
import java.io.IOException;

public class PostgresPrepareStatement extends PostgresBaseCursorStatement
{
    private String name;
    private String sql;
    private StatementNode stmt;

    @Override
    public PostgresStatement finishGenerating(PostgresServerSession server,
                                              String sql, StatementNode stmt,
                                              List<ParameterNode> params, int[] paramTypes) {
        PrepareStatementNode prepare = (PrepareStatementNode)stmt;
        this.name = prepare.getName();
        this.sql = sql.substring(stmt.getBeginOffset(), stmt.getEndOffset() + 1);
        this.stmt = prepare.getStatement();
        return this;
    }
    
    @Override
    public int execute(PostgresQueryContext context, int maxrows) throws IOException {
        PostgresServerSession server = context.getServer();
        server.prepareStatement(name, sql, stmt, null, null);
        {        
            PostgresMessenger messenger = server.getMessenger();
            messenger.beginMessage(PostgresMessages.COMMAND_COMPLETE_TYPE.code());
            messenger.writeString("PREPARE");
            messenger.sendMessage();
        }
        return 0;
    }
    
    public boolean putInCache() {
        return false;
    }

}
