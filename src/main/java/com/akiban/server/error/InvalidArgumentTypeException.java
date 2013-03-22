
package com.akiban.server.error;

public class InvalidArgumentTypeException extends InvalidOperationException
{
    public InvalidArgumentTypeException (String msg)
    {
        super(ErrorCode.INVALID_ARGUMENT_TYPE, msg);
    }
}
