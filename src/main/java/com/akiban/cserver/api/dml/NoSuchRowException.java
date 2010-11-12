package com.akiban.cserver.api.dml;

import com.akiban.message.ErrorCode;

public final class NoSuchRowException extends DMLException {
    public NoSuchRowException(String message) {
        super(ErrorCode.NO_SUCH_ROW, message);
    }
}
