package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class GroupMissingIndexException extends InvalidOperationException {
    public GroupMissingIndexException (TableName group, String indexName) {
        super (ErrorCode.GROUP_MISSING_INDEX, group.getSchemaName(), group.getTableName(), indexName);
    }
}
