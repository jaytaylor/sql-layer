package com.akiban.cserver.api.dml;

import com.akiban.message.ErrorCode;

public final class NoSuchIndexException extends DMLException {
    public NoSuchIndexException(String message) {
        super(ErrorCode.NO_INDEX, message);
    }
}
