
package com.akiban.server.error;

public class UnsupportedFKIndexException extends InvalidOperationException {
    public UnsupportedFKIndexException() {
        super (ErrorCode.UNSUPPORTED_FK_INDEX);
    }
}
