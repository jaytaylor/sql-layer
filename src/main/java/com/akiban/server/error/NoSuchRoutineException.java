package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class NoSuchRoutineException extends BaseSQLException {
    public NoSuchRoutineException(String schemaName, String routineName) {
        super(ErrorCode.NO_SUCH_ROUTINE, schemaName, routineName, null);
    }
    
    public NoSuchRoutineException(TableName name) {
        super(ErrorCode.NO_SUCH_ROUTINE, name.getSchemaName(), name.getTableName(), null);
    }

}
