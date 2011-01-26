package com.akiban.cserver.api.dml;

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
