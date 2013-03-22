
package com.akiban.server.error;

public final class ForeignKeyConstraintDMLException extends InvalidOperationException {
    public ForeignKeyConstraintDMLException() {
        super(ErrorCode.FK_CONSTRAINT_VIOLATION);
    }
}
