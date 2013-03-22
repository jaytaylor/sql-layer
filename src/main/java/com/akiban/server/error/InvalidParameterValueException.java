

package com.akiban.server.error;

public class InvalidParameterValueException extends InvalidOperationException
{
    public InvalidParameterValueException ()
    {
        super(ErrorCode.INVALID_PARAMETER_VALUE);
    }
    
    public InvalidParameterValueException (String msg)
    {
        super(ErrorCode.INVALID_PARAMETER_VALUE, msg);
    }
}
