
package com.akiban.server.error;

public class UnknownDataTypeException extends InvalidOperationException {
    public UnknownDataTypeException (String encoding) {
        super (ErrorCode.UNKNOWN_TYPE, encoding);
    }
}
