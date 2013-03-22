
package com.akiban.server.error;

public class ISTableVersionMismatchException extends InvalidOperationException {
    public ISTableVersionMismatchException(Integer storedVersion, Integer newVersion) {
        super(ErrorCode.IS_TABLE_VERSION_MISMATCH, storedVersion, newVersion);
    }
}
