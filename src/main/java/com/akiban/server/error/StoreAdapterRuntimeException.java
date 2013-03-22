
package com.akiban.server.error;

public class StoreAdapterRuntimeException extends InvalidOperationException
{
    public StoreAdapterRuntimeException(ErrorCode code, Object... args)
    {
        super(code, args);
    }
}
