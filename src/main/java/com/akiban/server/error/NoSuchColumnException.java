
package com.akiban.server.error;

import com.akiban.sql.parser.QueryTreeNode;

public final class NoSuchColumnException extends BaseSQLException {
    public NoSuchColumnException(String columnName) {
        this(columnName, null);
    }

    public NoSuchColumnException(String columnName, QueryTreeNode referenceNode) {
        super(ErrorCode.NO_SUCH_COLUMN, columnName, referenceNode);
    }
}
