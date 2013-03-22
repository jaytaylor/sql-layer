
package com.akiban.server.error;

public final class ViewReferencesExist extends InvalidOperationException {
    public ViewReferencesExist (String viewSchema, String viewName, String schemaName, String tableName) {
        super(ErrorCode.VIEW_REFERENCES_EXIST, viewSchema, viewName, schemaName, tableName);
    }
}
