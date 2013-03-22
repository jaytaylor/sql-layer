
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public final class ProtectedTableDDLException extends InvalidOperationException {
    public ProtectedTableDDLException(TableName name) {
        super(ErrorCode.PROTECTED_TABLE, name.getTableName(), name.getSchemaName());
    }
}
