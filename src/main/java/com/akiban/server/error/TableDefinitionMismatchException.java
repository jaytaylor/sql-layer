
package com.akiban.server.error;

import com.akiban.server.encoding.EncodingException;

public final class TableDefinitionMismatchException extends InvalidOperationException {
    public TableDefinitionMismatchException (Integer rowTableID, Integer tableID) {
        super(ErrorCode.TABLEDEF_MISMATCH, rowTableID, tableID);
    }

    public TableDefinitionMismatchException(EncodingException e) {
        super(ErrorCode.TABLEDEF_MISMATCH, "Couldn't encode a value; you probably gave a wrong type", e);
    }
}
