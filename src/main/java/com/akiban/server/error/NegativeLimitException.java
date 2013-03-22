
package com.akiban.server.error;

public final class NegativeLimitException extends InvalidOperationException {
    public NegativeLimitException(String name, int value) {
        super (ErrorCode.NEGATIVE_LIMIT, name, value);
    }
}
