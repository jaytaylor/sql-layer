
package com.akiban.server.error;

public class OutOfRangeException extends InvalidOperationException
{
    public OutOfRangeException(String val)
    {
        super(ErrorCode.VALUE_OUT_OF_RANGE, val);
    }
}
