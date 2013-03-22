
package com.akiban.server.error;

import com.akiban.server.api.dml.scan.CursorId;

public final class ConcurrentScanAndUpdateException extends InvalidOperationException {
    public ConcurrentScanAndUpdateException(CursorId cursorId) {
        super(ErrorCode.CONCURRENT_MODIFICATION, cursorId);
    }
}
