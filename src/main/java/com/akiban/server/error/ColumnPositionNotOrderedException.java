
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class ColumnPositionNotOrderedException extends
        InvalidOperationException {
    public ColumnPositionNotOrderedException (TableName table, String columnName, int position, int index) {
        super (ErrorCode.COLUMN_POS_ORDERED, table.getSchemaName(), table.getTableName(), columnName, position, index);
    }
}
