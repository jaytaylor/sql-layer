package com.akiban.cserver.api.dml;

import com.akiban.message.ErrorCode;

public final class ForeignKeyConstraintDMLException extends DMLException {
    public ForeignKeyConstraintDMLException(String message) {
        super(ErrorCode.FK_CONSTRAINT_VIOLATION, message);
    }
}
