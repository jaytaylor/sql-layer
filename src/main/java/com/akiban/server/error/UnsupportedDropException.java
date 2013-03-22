
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public final class UnsupportedDropException extends InvalidOperationException {
    public UnsupportedDropException(TableName name) {
        super(ErrorCode.UNSUPPORTED_DROP, name.getSchemaName(), name.getTableName());
    }
}
