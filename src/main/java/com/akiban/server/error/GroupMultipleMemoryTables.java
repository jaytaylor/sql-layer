
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class GroupMultipleMemoryTables extends InvalidOperationException {
    
    public GroupMultipleMemoryTables (TableName parentTable, TableName childTable) {
        super (ErrorCode.GROUP_MULTIPLE_MEM_TABLES, 
                parentTable.getSchemaName(), parentTable.getTableName(),
                childTable.getSchemaName(), childTable.getTableName());
    }

}
