
package com.akiban.server.error;

public class DuplicateSchemaException extends InvalidOperationException {
    public DuplicateSchemaException (String schemaName) {
        super (ErrorCode.DUPLICATE_SCHEMA, schemaName);
    }
}
