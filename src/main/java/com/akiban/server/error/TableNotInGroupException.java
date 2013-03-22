
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class TableNotInGroupException extends InvalidOperationException {
    public TableNotInGroupException (TableName table) {
        super(ErrorCode.TABLE_NOT_IN_GROUP, table.getSchemaName(), table.getTableName());
    }
}
