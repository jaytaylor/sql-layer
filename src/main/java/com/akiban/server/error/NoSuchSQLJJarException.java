package com.akiban.server.error;

import com.akiban.ais.model.TableName;

public class NoSuchSQLJJarException extends BaseSQLException {
    public NoSuchSQLJJarException(String schemaName, String jarName) {
        super(ErrorCode.NO_SUCH_SQLJ_JAR, schemaName, jarName, null);
    }
    
    public NoSuchSQLJJarException(TableName name) {
        super(ErrorCode.NO_SUCH_SQLJ_JAR, name.getSchemaName(), name.getTableName(), null);
    }

}
