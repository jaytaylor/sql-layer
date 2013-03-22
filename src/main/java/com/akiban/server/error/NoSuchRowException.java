
package com.akiban.server.error;

import com.persistit.Key;

public final class NoSuchRowException extends InvalidOperationException {

    public NoSuchRowException (Key key) {
        super(ErrorCode.NO_SUCH_ROW, key);
    }
}
