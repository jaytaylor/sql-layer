package com.akiban.server.error;

public class SchemaLoadIOException extends InvalidOperationException {
    public SchemaLoadIOException (String message) {
        super (ErrorCode.SCHEMA_LOAD_IO_ERROR, message);
    }
}
