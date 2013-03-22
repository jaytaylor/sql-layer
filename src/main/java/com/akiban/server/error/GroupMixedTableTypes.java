
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class GroupMixedTableTypes extends InvalidOperationException {

    public GroupMixedTableTypes (TableName groupName, boolean isMemoryTable, TableName table1) {
        super (ErrorCode.GROUP_MIXED_TABLE_TYPES,
               groupName.getSchemaName(), groupName.getTableName(),
               isMemoryTable ? "is": "is not",
               table1.getSchemaName(), table1.getTableName());
    }
}
