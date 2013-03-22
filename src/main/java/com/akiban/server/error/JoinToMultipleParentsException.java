
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public final class JoinToMultipleParentsException extends InvalidOperationException {
    public JoinToMultipleParentsException (TableName name) {
        super (ErrorCode.JOIN_TO_MULTIPLE_PARENTS, name.getSchemaName(), name.getTableName());
    }
}
