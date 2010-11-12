package com.akiban.cserver.api.dml;

import com.akiban.message.ErrorCode;

public final class CursorIsFinishedException extends DMLException {
    public CursorIsFinishedException(String message) {
        super(ErrorCode.CURSOR_IS_FINISHED, message);
    }
}
