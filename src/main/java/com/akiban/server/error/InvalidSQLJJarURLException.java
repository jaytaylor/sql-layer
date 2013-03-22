

package com.akiban.server.error;

public class InvalidSQLJJarURLException extends InvalidOperationException
{
    public InvalidSQLJJarURLException(String schemaName, String jarName, String msg)
    {
        super(ErrorCode.INVALID_SQLJ_JAR_URL, schemaName, jarName, msg);
    }

    public InvalidSQLJJarURLException(String schemaName, String jarName, Throwable cause)
    {
        super(ErrorCode.INVALID_SQLJ_JAR_URL, schemaName, jarName, cause.toString());
        initCause(cause);
    }
}
