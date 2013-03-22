

package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class ExternalRoutineInvocationException extends InvalidOperationException
{
    public ExternalRoutineInvocationException(String schemaName, String routineName, String msg)
    {
        super(ErrorCode.EXTERNAL_ROUTINE_INVOCATION, schemaName, routineName, msg);
    }

    public ExternalRoutineInvocationException(TableName name, String msg)
    {
        this(name.getSchemaName(), name.getTableName(), msg);
    }

    public ExternalRoutineInvocationException(TableName name, Throwable cause)
    {
        this(name, cause.toString());
        initCause(cause);
    }
}
