
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class WrongTableForIndexException extends InvalidOperationException {
    public WrongTableForIndexException (TableName table) {
        super (ErrorCode.WRONG_TABLE_FOR_INDEX, table.getSchemaName(), table.getTableName());
    }
}
