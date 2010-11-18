package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.dml.DMLException;
import com.akiban.message.ErrorCode;

public final class CursorIsFinishedException extends DMLException {
    public CursorIsFinishedException(CursorId cursor) {
        super(ErrorCode.CURSOR_IS_FINISHED, "Unknown cursor: %s", cursor);
    }

    public CursorIsFinishedException(InvalidOperationException e) {
        super(e);
    }
}
