package com.akiban.server.error;

public class DisplayFilterSetException extends InvalidOperationException {
    public DisplayFilterSetException (String message) {
        super (ErrorCode.SET_FILTER_FAIL, message);
    }
}
