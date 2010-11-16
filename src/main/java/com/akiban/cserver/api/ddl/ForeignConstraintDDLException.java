package com.akiban.cserver.api.ddl;

import com.akiban.cserver.InvalidOperationException;

public final class ForeignConstraintDDLException extends DDLException {
    public ForeignConstraintDDLException(InvalidOperationException e) {
        super(e);
    }
}
