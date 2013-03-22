
package com.akiban.server.error;

public class InvalidCharToNumException extends InvalidOperationException {
    public InvalidCharToNumException (String source) {
        super (ErrorCode.INVALID_CHAR_TO_NUM, source);
    }
}
