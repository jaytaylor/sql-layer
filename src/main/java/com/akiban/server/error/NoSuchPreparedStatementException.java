
package com.akiban.server.error;

import com.akiban.sql.parser.QueryTreeNode;

public final class NoSuchPreparedStatementException extends BaseSQLException {
    public NoSuchPreparedStatementException(String columnName) {
        this(columnName, null);
    }

    public NoSuchPreparedStatementException(String columnName, QueryTreeNode referenceNode) {
        super(ErrorCode.NO_SUCH_PREPARED_STATEMENT, columnName, referenceNode);
    }
}
