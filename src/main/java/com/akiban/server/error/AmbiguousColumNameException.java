
package com.akiban.server.error;

import com.akiban.sql.parser.QueryTreeNode;

public class AmbiguousColumNameException extends BaseSQLException {
    public AmbiguousColumNameException (String columnName, QueryTreeNode referenceNode) {
        super(ErrorCode.AMBIGUOUS_COLUMN_NAME, columnName, referenceNode);
    }
}
