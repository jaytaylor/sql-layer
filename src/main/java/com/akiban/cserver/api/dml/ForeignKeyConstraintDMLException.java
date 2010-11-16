package com.akiban.cserver.api.dml;

import com.akiban.cserver.InvalidOperationException;

public final class ForeignKeyConstraintDMLException extends DMLException {
    public ForeignKeyConstraintDMLException(InvalidOperationException e) {
    super(e);
    }
}
