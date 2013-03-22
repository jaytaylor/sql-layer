
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class JoinColumnTypesMismatchException extends InvalidOperationException {
    //Column `%s`.`%s`.`%s` type used for join does not match parent column `%s`.`%s`.`%s`
    public JoinColumnTypesMismatchException (TableName childTable, String childColumn, 
            TableName parentTable, String parentColumn) {
        super(ErrorCode.JOIN_TYPE_MISMATCH,
                childTable.getSchemaName(), childTable.getTableName(), childColumn,
                parentTable.getSchemaName(), parentTable.getTableName(), parentColumn);
    }
}
