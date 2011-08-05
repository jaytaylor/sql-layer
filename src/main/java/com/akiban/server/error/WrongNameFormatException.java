package com.akiban.server.error;

public class WrongNameFormatException extends InvalidOperationException {
    public WrongNameFormatException (String badName) {
        super (ErrorCode.WRONG_NAME_FORMAT, badName);
    }
}
