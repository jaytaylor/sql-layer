
package com.akiban.server.error;

public final class UnsupportedCharsetException extends InvalidOperationException {
    public UnsupportedCharsetException (String schemaName, String tableName, String charsetName) {
        super(ErrorCode.UNSUPPORTED_CHARSET, schemaName, tableName, charsetName);
    }
}
