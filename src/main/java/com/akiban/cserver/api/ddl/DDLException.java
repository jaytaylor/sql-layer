package com.akiban.cserver.api.ddl;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.message.ErrorCode;

public abstract class DDLException extends InvalidOperationException {
    protected DDLException(InvalidOperationException cause) {
        super(cause.getCode(), cause.getMessage(), cause);
    }

    protected DDLException(ErrorCode code, String message) {
        super(code, message);
    }

    protected DDLException(ErrorCode code, String formatter, Object... args) {
        super(code, formatter, args);
    }
}
