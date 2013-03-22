
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class UnsupportedIndexDataTypeException extends InvalidOperationException {
    //Table `%s`.`%s` index `%s` has unsupported type `%s` from column `%s`
    public UnsupportedIndexDataTypeException (TableName table, String index, String columnName, String typeName) {
        super (ErrorCode.UNSUPPORTED_INDEX_DATA_TYPE, 
                table.getSchemaName(), table.getTableName(),
                index,
                typeName,
                columnName);
    }
}
