

package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class SQLJInstanceException extends InvalidOperationException
{
    public SQLJInstanceException(String schemaName, String jarName, String msg)
    {
        super(ErrorCode.SQLJ_INSTANCE_EXCEPTION, schemaName, jarName, msg);
    }

    public SQLJInstanceException(TableName name, String msg)
    {
        this(name.getSchemaName(), name.getTableName(), msg);
    }

    public SQLJInstanceException(TableName name, Throwable cause)
    {
        this(name, cause.toString());
        initCause(cause);
    }
}
