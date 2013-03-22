
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class BranchingGroupIndexException extends InvalidOperationException {
    public BranchingGroupIndexException (String indexName, TableName table1, TableName table2) {
        super(ErrorCode.BRANCHING_GROUP_INDEX, indexName, 
                table1.getSchemaName(), table1.getTableName(), 
                table2.getSchemaName(), table2.getTableName());
    }

}
