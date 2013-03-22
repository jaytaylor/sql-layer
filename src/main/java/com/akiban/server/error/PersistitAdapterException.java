
package com.akiban.server.error;

public class PersistitAdapterException extends StoreAdapterRuntimeException {
    public PersistitAdapterException(Throwable ex) {
        super(ErrorCode.PERSISTIT_ERROR, ex.getMessage());
        initCause(ex);
    }
}
