
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public final class ForeignConstraintDDLException extends InvalidOperationException {
    public ForeignConstraintDDLException(TableName parent, TableName child) {
        super(ErrorCode.FK_DDL_VIOLATION,
                parent.getSchemaName(), parent.getTableName(), 
                child.getSchemaName(), child.getTableName());
    }
}
