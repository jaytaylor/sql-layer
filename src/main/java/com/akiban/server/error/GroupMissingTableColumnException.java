
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class GroupMissingTableColumnException extends InvalidOperationException {
    public GroupMissingTableColumnException (TableName groupTable, TableName userTable, String columnName) {
        super(ErrorCode.GROUP_MISSING_COL, groupTable.getSchemaName(), groupTable.getTableName(),
                userTable.getSchemaName(), userTable.getTableName(), columnName);
    }
}
