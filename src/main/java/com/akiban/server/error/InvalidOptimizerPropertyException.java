
package com.akiban.server.error;

public class InvalidOptimizerPropertyException extends InvalidOperationException
{
    public InvalidOptimizerPropertyException(String key, String value)
    {
        super(ErrorCode.INVALID_OPTIMIZER_PROPERTY, key, value);
    }
}
