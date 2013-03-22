
package com.akiban.server.api.dml;

public class DMLError extends Error
{
    public DMLError(String message)
    {
        super(message);
    }

    public DMLError(String message, Throwable cause)
    {
        super(message, cause);
    }
}
