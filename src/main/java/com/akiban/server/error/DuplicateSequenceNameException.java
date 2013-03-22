package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class DuplicateSequenceNameException extends InvalidOperationException {

    public DuplicateSequenceNameException(TableName name) {
        super(ErrorCode.DUPLICATE_SEQUENCE, name.getSchemaName(), name.getTableName());
    }
}
