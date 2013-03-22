
package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public final class DuplicateSQLJJarNameException extends InvalidOperationException {
    public DuplicateSQLJJarNameException(TableName name) {
        super(ErrorCode.DUPLICATE_SQLJ_JAR, name.getSchemaName(), name.getTableName());
    }
    
    public DuplicateSQLJJarNameException(String schemaName, String jarName)
    {
        super(ErrorCode.DUPLICATE_SQLJ_JAR, schemaName, jarName);
    }
}
