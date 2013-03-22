
package com.akiban.server.error;

public class NameIsNullException extends InvalidOperationException {
    public NameIsNullException (String source, String type) {
        super (ErrorCode.NAME_IS_NULL, source, type);
    }
}
