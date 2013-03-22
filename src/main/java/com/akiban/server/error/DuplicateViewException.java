
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class DuplicateViewException extends InvalidOperationException {
    public DuplicateViewException (TableName viewName) {
        super(ErrorCode.DUPLICATE_VIEW, viewName.getSchemaName(), viewName.getTableName());
    }
    
    public DuplicateViewException(String schemaName, String viewName) {
        super(ErrorCode.DUPLICATE_VIEW, schemaName, viewName);
    }
}
