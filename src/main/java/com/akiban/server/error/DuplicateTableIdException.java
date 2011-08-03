package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class DuplicateTableIdException extends InvalidOperationException {
    public DuplicateTableIdException (TableName name1, TableName name2) {
        super (ErrorCode.DUPLICATE_TABLEID, 
                name1.getSchemaName(), name1.getTableName(),
                name2.getSchemaName(), name2.getTableName());
    }
}
