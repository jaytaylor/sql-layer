package com.akiban.server.error;

public class UnsupportedParametersException extends InvalidOperationException {
    public UnsupportedParametersException() {
        super(ErrorCode.UNSUPPORTED_PARAMETERS);
    }
}
