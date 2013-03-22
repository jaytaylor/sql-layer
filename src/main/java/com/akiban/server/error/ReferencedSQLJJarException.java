package com.akiban.server.error;

import com.akiban.ais.model.SQLJJar;

public class ReferencedSQLJJarException extends InvalidOperationException {
    public ReferencedSQLJJarException(SQLJJar sqljJar) {
        super(ErrorCode.REFERENCED_SQLJ_JAR, sqljJar.getName().toString());
    }

}
