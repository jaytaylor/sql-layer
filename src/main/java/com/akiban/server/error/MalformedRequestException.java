package com.akiban.server.error;

public class MalformedRequestException extends InvalidOperationException {
    public MalformedRequestException (String reason) {
        super (ErrorCode.MALFORMED_REQUEST, reason);
    }
}
