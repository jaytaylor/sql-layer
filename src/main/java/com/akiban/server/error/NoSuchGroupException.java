
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public final class NoSuchGroupException extends InvalidOperationException {
    public NoSuchGroupException(TableName groupName) {
        super(ErrorCode.NO_SUCH_GROUP, groupName.getSchemaName(), groupName.getTableName());
    }
}
