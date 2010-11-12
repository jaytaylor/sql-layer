package com.akiban.cserver.api.ddl;

import com.akiban.message.ErrorCode;

public final class UnsupportedCharsetException extends DDLException {
    public UnsupportedCharsetException(String message) {
        super(ErrorCode.UNSUPPORTED_CHARSET, message);
    }
}
