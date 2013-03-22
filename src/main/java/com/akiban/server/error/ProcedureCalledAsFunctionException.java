package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class ProcedureCalledAsFunctionException extends BaseSQLException {
    public ProcedureCalledAsFunctionException(String schemaName, String routineName) {
        super(ErrorCode.PROCEDURE_CALLED_AS_FUNCTION, schemaName, routineName, null);
    }
    
    public ProcedureCalledAsFunctionException(TableName name) {
        super(ErrorCode.PROCEDURE_CALLED_AS_FUNCTION, name.getSchemaName(), name.getTableName(), null);
    }

}
