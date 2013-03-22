
package com.akiban.server.error;

public final class UnsupportedModificationException extends InvalidOperationException {
    public UnsupportedModificationException() {
    super(ErrorCode.UNSUPPORTED_MODIFICATION);
    }
}
