
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public final class JoinToWrongColumnsException extends InvalidOperationException {
    //Table `%s`.`%s` join reference part `%s` does not match `%s`.`%s` primary key part `%s`

    public JoinToWrongColumnsException(TableName table, String columnName, TableName joinTo, String joinToColumn) {
    super(ErrorCode.JOIN_TO_WRONG_COLUMNS, 
            table.getSchemaName(), table.getTableName(),
            columnName, 
            joinTo.getSchemaName(), joinTo.getTableName(), 
            joinToColumn);
    }
}
