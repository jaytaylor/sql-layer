package com.akiban.cserver.api.dml;

import com.akiban.message.ErrorCode;

public final class UnsupportedModificationException extends DMLException {
    public UnsupportedModificationException(String message) {
        super(ErrorCode.UNSUPPORTED_MODIFICATION, message);
    }
}
