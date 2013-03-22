
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public final class DuplicateRoutineNameException extends InvalidOperationException {
    public DuplicateRoutineNameException(TableName name) {
        super(ErrorCode.DUPLICATE_ROUTINE, name.getSchemaName(), name.getTableName());
    }
    
    public DuplicateRoutineNameException(String schemaName, String routineName)
    {
        super(ErrorCode.DUPLICATE_ROUTINE, schemaName, routineName);
    }
}
