
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class JoinColumnMismatchException extends InvalidOperationException {
    //Join column list size (%d) for table `%s`.`%s` does not match table `%s`.`%s` primary key (%d)
    public JoinColumnMismatchException (int listSize, 
            TableName childTable, 
            TableName parentTable,
            int pkSize) {
        super (ErrorCode.JOIN_COLUMN_MISMATCH, listSize, childTable.getSchemaName(), childTable.getTableName(),
                parentTable.getSchemaName(), parentTable.getTableName(), pkSize);
    }
}
