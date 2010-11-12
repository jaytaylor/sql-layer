package com.akiban.cserver.api.ddl;

import com.akiban.message.ErrorCode;

public final class ForeignConstraintDDLException extends DDLException {
    public ForeignConstraintDDLException(String message) {
        super(ErrorCode.FK_CONSTRAINT_VIOLATION, message);
    }
}
