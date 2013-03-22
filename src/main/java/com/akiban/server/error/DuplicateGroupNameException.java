
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class DuplicateGroupNameException extends InvalidOperationException {
    public DuplicateGroupNameException(TableName groupName) {
        super(ErrorCode.DUPLICATE_GROUP, groupName.getSchemaName(), groupName.getTableName());
    }
}
