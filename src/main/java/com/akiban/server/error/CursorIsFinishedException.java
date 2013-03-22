
package com.akiban.server.error;

import com.akiban.server.api.dml.scan.CursorId;

public final class CursorIsFinishedException extends InvalidOperationException {
    //Finished scan cursor requested more rows: %s
    public CursorIsFinishedException(CursorId cursor) {
        super(ErrorCode.CURSOR_IS_FINISHED, cursor);
    }
}
