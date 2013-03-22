
package com.akiban.server.error;

public class UnsupportedCheckConstraintException extends
        InvalidOperationException {
    public UnsupportedCheckConstraintException () {
        super (ErrorCode.UNSUPPORTED_CHECK);
    }
}
