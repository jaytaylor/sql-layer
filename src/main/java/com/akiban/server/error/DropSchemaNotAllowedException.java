
package com.akiban.server.error;

public class DropSchemaNotAllowedException extends InvalidOperationException {
    public DropSchemaNotAllowedException (String schemaName) {
        super (ErrorCode.DROP_SCHEMA_NOT_ALLOWED, schemaName);
    }
}
