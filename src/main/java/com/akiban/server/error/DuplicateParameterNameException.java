
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public final class DuplicateParameterNameException extends InvalidOperationException {
    public DuplicateParameterNameException(TableName procedureName, String parameterName) {
        super(ErrorCode.DUPLICATE_PARAMETER, procedureName.getSchemaName(), procedureName.getTableName(), parameterName);
    }
}
