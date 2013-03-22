
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class UndefinedViewException extends InvalidOperationException {
    public UndefinedViewException(TableName viewName) {
        super(ErrorCode.UNDEFINED_VIEW, viewName.getSchemaName(), viewName.getTableName());
    }

    public UndefinedViewException(String schemaName, String viewName) {
        super(ErrorCode.UNDEFINED_VIEW, schemaName, viewName);
    }
}
