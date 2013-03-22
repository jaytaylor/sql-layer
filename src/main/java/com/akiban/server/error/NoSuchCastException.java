
package com.akiban.server.error;

import com.akiban.server.types3.TInstance;

public final class NoSuchCastException extends InvalidOperationException {
    public NoSuchCastException(TInstance source, TInstance target) {
        super(ErrorCode.NO_SUCH_CAST, source.toString(), target.toString());
    }
}
