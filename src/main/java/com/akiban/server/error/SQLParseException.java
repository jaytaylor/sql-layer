
package com.akiban.server.error;

import com.akiban.sql.parser.SQLParserException;

public final class SQLParseException extends BaseSQLException
{
    public SQLParseException(SQLParserException cause) {
        super(ErrorCode.SQL_PARSE_EXCEPTION, cause.getMessage(), cause.getErrorPosition());
        initCause(cause);
    }
}
