
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class NoSuchGroupingFKException extends InvalidOperationException {
    public NoSuchGroupingFKException(TableName tableName) {
        super(ErrorCode.NO_SUCH_GROUPING_FK, tableName.getSchemaName(), tableName.getTableName());
    }
}
