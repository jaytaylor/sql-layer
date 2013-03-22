
package com.akiban.server.error;

import com.akiban.sql.StandardException;

public final class SQLParserInternalException extends InvalidOperationException {
    public SQLParserInternalException(StandardException cause) {
        super(ErrorCode.SQL_PARSER_INTERNAL_EXCEPTION, cause.getMessage());
        initCause(cause);
    }
}
