
package com.akiban.server.error;

public class UnsupportedCreateSelectException extends
        InvalidOperationException {
    public UnsupportedCreateSelectException () {
        super (ErrorCode.UNSUPPORTED_CREATE_SELECT);
    }
}
