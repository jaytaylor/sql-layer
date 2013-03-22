
package com.akiban.server.error;

public final class SchemaDefParseException extends InvalidOperationException {
    public SchemaDefParseException(String schemaName, String message, String ddl) {
        super(ErrorCode.SCHEMA_DEF_PARSE_EXCEPTION, schemaName, message, ddl);
    }
}
