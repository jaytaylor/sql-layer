package com.akiban.cserver.api.common;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.message.ErrorCode;

public final class ResolutionException extends RuntimeException {
    public ResolutionException() {
        super();
    }

    public ResolutionException(Object message) {
        super(message == null ? null : message.toString());
    }
}
