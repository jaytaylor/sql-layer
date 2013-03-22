
package com.akiban.server.error;

public final class ZeroDateTimeException extends InvalidOperationException {
    public ZeroDateTimeException() {
        super(ErrorCode.ZERO_DATE_TIME);
    }
}
