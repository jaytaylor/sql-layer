

package com.akiban.server.error;

public class InvalidRoutineException extends InvalidOperationException
{
    public InvalidRoutineException(String schemaName, String routineName, String msg)
    {
        super(ErrorCode.INVALID_ROUTINE, schemaName, routineName, msg);
    }
}
