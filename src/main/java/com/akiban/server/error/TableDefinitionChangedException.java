
package com.akiban.server.error;

import com.akiban.server.api.dml.scan.CursorId;

public final class TableDefinitionChangedException extends InvalidOperationException {
    public TableDefinitionChangedException(CursorId cursorId) {
        super(ErrorCode.TABLE_DEFINITION_CHANGED, cursorId);
    }
}
