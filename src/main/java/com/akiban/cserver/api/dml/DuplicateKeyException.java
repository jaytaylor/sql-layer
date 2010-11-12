package com.akiban.cserver.api.dml;

import com.akiban.message.ErrorCode;

public final class DuplicateKeyException extends DMLException {
    public DuplicateKeyException(String message) {
        super(ErrorCode.DUPLICATE_KEY, message);
    }
}
