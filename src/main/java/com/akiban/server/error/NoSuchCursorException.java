
package com.akiban.server.error;

import com.akiban.sql.parser.QueryTreeNode;

public final class NoSuchCursorException extends BaseSQLException {
    public NoSuchCursorException(String columnName) {
        this(columnName, null);
    }

    public NoSuchCursorException(String columnName, QueryTreeNode referenceNode) {
        super(ErrorCode.NO_SUCH_CURSOR, columnName, referenceNode);
    }
}
