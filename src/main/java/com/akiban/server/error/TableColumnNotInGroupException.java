
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class TableColumnNotInGroupException extends InvalidOperationException {
    public TableColumnNotInGroupException (TableName name, String columnName) {
        super (ErrorCode.TABLE_COL_IN_GROUP, name.getSchemaName(), name.getTableName(), columnName);
    }
}
