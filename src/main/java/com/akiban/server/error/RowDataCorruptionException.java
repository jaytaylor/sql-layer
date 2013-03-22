
package com.akiban.server.error;

import com.persistit.Key;

public class RowDataCorruptionException extends InvalidOperationException {
    public RowDataCorruptionException (Key key) {
        super (ErrorCode.INTERNAL_CORRUPTION, key);
    }

}
