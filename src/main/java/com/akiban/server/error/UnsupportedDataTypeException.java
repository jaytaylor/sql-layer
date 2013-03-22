
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class UnsupportedDataTypeException extends InvalidOperationException {
    //Table `%s`.`%s` has column `%s` with unsupported data type `%s`
    public UnsupportedDataTypeException(TableName table, String columnName, String type) {
        super(ErrorCode.UNSUPPORTED_DATA_TYPE,
                table.getSchemaName(), table.getTableName(), 
                columnName, type);
    }
}
