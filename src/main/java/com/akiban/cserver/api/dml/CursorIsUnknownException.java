package com.akiban.cserver.api.dml;

import com.akiban.message.ErrorCode;

public final class CursorIsUnknownException extends DMLException {
    public CursorIsUnknownException(String message) {
        super(ErrorCode.CURSOR_IS_UNKNOWN, message);
    }
}
