

package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class InvalidSQLJDeploymentDescriptorException extends InvalidOperationException
{
    public InvalidSQLJDeploymentDescriptorException(String schemaName, String jarName, String msg)
    {
        super(ErrorCode.INVALID_SQLJ_DEPLOYMENT_DESCRIPTOR, schemaName, jarName, msg);
    }

    public InvalidSQLJDeploymentDescriptorException(TableName name, String msg) 
    {
        this(name.getSchemaName(), name.getTableName(), msg);
    }

    public InvalidSQLJDeploymentDescriptorException(TableName name, Throwable cause) 
    {
        this(name, cause.toString());
        initCause(cause);
    }
}
