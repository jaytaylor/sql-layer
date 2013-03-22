
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class DropGroupNotRootException extends InvalidOperationException {
    public DropGroupNotRootException (TableName tableName) {
        super (ErrorCode.DROP_GROUP_NOT_ROOT, tableName.getSchemaName(), tableName.getTableName());
    }

}
