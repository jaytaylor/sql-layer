
package com.akiban.sql.embedded;

import com.akiban.sql.server.ServerStatement;

abstract class ExecutableStatement implements ServerStatement
{
    public abstract ExecuteResults execute(EmbeddedQueryContext context);

    public JDBCResultSetMetaData getResultSetMetaData() {
        return null;
    }

    public JDBCParameterMetaData getParameterMetaData() {
        return null;
    }

    public long getEstimatedRowCount() {
        return -1;
    }

}
