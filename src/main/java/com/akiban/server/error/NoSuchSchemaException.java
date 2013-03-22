
package com.akiban.server.error;

public class NoSuchSchemaException extends InvalidOperationException {
    public NoSuchSchemaException (String schemaName) {
        super (ErrorCode.NO_SUCH_SCHEMA, schemaName);
    }
}
