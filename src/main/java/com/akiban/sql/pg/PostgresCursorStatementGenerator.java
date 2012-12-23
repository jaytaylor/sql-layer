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

import com.akiban.sql.parser.NodeTypes;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.StatementNode;

import java.util.List;

/** SQL statements related to manipulation of prepared statements and cursors. */
public class PostgresCursorStatementGenerator extends PostgresBaseStatementGenerator
{
    public PostgresCursorStatementGenerator(PostgresServerSession server) {
    }

    @Override
    public PostgresStatement generateStub(PostgresServerSession server,
                                          String sql, StatementNode stmt,
                                          List<ParameterNode> params, int[] paramTypes)  {
        switch (stmt.getNodeType()) {
        case NodeTypes.PREPARE_STATEMENT_NODE:
            return new PostgresPrepareStatement();
        case NodeTypes.DECLARE_STATEMENT_NODE:
            return new PostgresDeclareStatement();
        case NodeTypes.EXECUTE_STATEMENT_NODE:
            return new PostgresExecuteStatement();
        case NodeTypes.FETCH_STATEMENT_NODE:
            return new PostgresFetchStatement();
        case NodeTypes.CLOSE_STATEMENT_NODE:
        case NodeTypes.DEALLOCATE_STATEMENT_NODE:
            return new PostgresCloseCursorStatement();
        default:
            return null;
        }
    }
}
