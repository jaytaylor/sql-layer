package com.akiban.cserver.api.dml;

import com.akiban.message.ErrorCode;

public final class RowOutputException extends DMLException {
    public RowOutputException(String message) {
        super(ErrorCode.ROW_OUTPUT, message);
    }
}
