
package com.akiban.server.error;

import com.akiban.ais.model.Group;

public class GroupTreeNameIsNullException extends InvalidOperationException {
    public GroupTreeNameIsNullException(Group group) {
        super(ErrorCode.GROUP_TREE_NAME_IS_NULL,
              group.getRoot().getName().getSchemaName(),
              group.getRoot().getName().getTableName());
    }
}
