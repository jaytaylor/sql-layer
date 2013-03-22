
package com.akiban.server.error;

import com.akiban.ais.model.Table;

public class ReferencedTableException extends InvalidOperationException {
    public ReferencedTableException (Table table) {
        super (ErrorCode.REFERENCED_TABLE, table.getName().toString());
    }
}
