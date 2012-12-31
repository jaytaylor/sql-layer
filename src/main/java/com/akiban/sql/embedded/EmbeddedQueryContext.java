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

package com.akiban.sql.embedded;

import com.akiban.sql.server.ServerQueryContext;

import com.akiban.server.error.ErrorCode;

public class EmbeddedQueryContext extends ServerQueryContext<JDBCConnection>
{
    private JDBCStatement statement;
    private JDBCResultSet resultSet;

    protected EmbeddedQueryContext(JDBCConnection connection) {
        super(connection);
    }

    protected EmbeddedQueryContext(JDBCStatement statement) {
        super(statement.connection);
        this.statement = statement;
    }

    protected EmbeddedQueryContext(JDBCResultSet resultSet) {
        super(resultSet.statement.connection);
        this.statement = resultSet.statement;
        this.resultSet = resultSet;
    }

    @Override
    public void notifyClient(NotificationLevel level, ErrorCode errorCode, String message) {
        if (getServer().shouldNotify(level)) {
            JDBCWarning warning = new JDBCWarning(level, errorCode, message);
            // If we are associated with a particular result set /
            // statement, direct warning there.
            if (resultSet != null)
                resultSet.addWarning(warning);
            else if (statement != null)
                statement.addWarning(warning);
            else
                getServer().addWarning(warning);
        }
    }

}
