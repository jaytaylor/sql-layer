
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class GroupHasMultipleRootsException extends InvalidOperationException {
    public GroupHasMultipleRootsException (TableName groupName, TableName table1, TableName table2) {
        super(ErrorCode.GROUP_MULTIPLE_ROOTS,
              groupName.getSchemaName(), groupName.getTableName(),
              table1.getSchemaName(), table1.getTableName(),
              table2.getSchemaName(), table2.getTableName());
    }
}
