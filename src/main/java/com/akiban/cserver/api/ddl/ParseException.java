package com.akiban.cserver.api.ddl;

import com.akiban.message.ErrorCode;

public final class ParseException extends DDLException {
    ParseException(String message) {
        super(ErrorCode.PARSE_EXCEPTION, message);
    }
}
