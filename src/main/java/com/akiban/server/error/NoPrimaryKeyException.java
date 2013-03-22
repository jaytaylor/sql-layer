
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class NoPrimaryKeyException extends InvalidOperationException {
    public NoPrimaryKeyException (TableName name) {
        super(ErrorCode.MISSING_PRIMARY_KEY, name.getSchemaName(), name.getTableName());
    }
}
