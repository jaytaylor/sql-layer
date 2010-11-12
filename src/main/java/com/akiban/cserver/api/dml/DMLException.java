package com.akiban.cserver.api.dml;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.message.ErrorCode;

public class DMLException extends InvalidOperationException {
    protected DMLException(ErrorCode code, String message) {
        super(code, message);
    }

    protected DMLException(ErrorCode code, String formatter, Object... args) {
        super(code, formatter, args);
    }
}
