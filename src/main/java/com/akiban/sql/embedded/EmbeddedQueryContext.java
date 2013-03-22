
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
