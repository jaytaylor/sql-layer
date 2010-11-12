package com.akiban.cserver.api.dml;

import com.akiban.message.ErrorCode;

public final class UnsupportedReadException extends DMLException {
    public UnsupportedReadException(String message) {
        super(ErrorCode.UNSUPPORTED_READ, message);
    }
}
