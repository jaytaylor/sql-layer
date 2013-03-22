package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class NoSuchSequenceException extends BaseSQLException {
    public NoSuchSequenceException (String schemaName, String sequenceName) {
        super (ErrorCode.NO_SUCH_SEQUENCE, schemaName, sequenceName, null);
    }
    
    public NoSuchSequenceException (TableName sequenceName) {
        super (ErrorCode.NO_SUCH_SEQUENCE, sequenceName.getSchemaName(), sequenceName.getTableName(), null);
    }

}
