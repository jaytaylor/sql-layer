
package com.akiban.server.error;

import com.akiban.sql.parser.QueryTreeNode;

public class UnsupportedSQLException extends BaseSQLException {
    public UnsupportedSQLException(String msg, QueryTreeNode sql) {
        super(ErrorCode.UNSUPPORTED_SQL, msg, sql);
    }
    
    public UnsupportedSQLException(String msg) {
        super(ErrorCode.UNSUPPORTED_SQL, msg, -1);
    }
}
