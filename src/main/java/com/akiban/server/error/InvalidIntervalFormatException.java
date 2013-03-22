
package com.akiban.server.error;

public class InvalidIntervalFormatException extends InvalidOperationException
{
    public InvalidIntervalFormatException(String type, String val)
    {
        super(ErrorCode.INVALID_INTERVAL_FORMAT, type, val);
    }
}
