package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.DMLException;
import com.akiban.message.ErrorCode;

public final class CursorIsUnknownException extends DMLException {

    public CursorIsUnknownException(TableId tableId) {
        super(ErrorCode.CURSOR_IS_UNKNOWN, "No cursor for tableId %s", tableId);
    }

    public CursorIsUnknownException(CursorId cursor) {
        super(ErrorCode.CURSOR_IS_UNKNOWN, "Unknown cursor: %s", cursor);
    }

    public CursorIsUnknownException(InvalidOperationException e) {
        super(e);
    }
}
