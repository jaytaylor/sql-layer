package com.akiban.cserver.api.dml;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.message.ErrorCode;

public class DMLException extends InvalidOperationException {
    protected DMLException(InvalidOperationException cause) {
        super(cause.getCode(), cause.getShortMessage(), cause);
    }

    protected DMLException(ErrorCode code, String message) {
        super(code, message);
    }

    protected DMLException(ErrorCode code, String message, Throwable cause) {
        super(code, message, cause);
    }

    protected DMLException(ErrorCode code, String formatter, Object... args) {
        super(code, formatter, args);
    }
}
