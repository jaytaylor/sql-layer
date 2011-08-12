package com.akiban.server.error;


public class AisSQLErrorException extends InvalidOperationException {
    public AisSQLErrorException (String source, String message) {
        super (ErrorCode.AIS_MYSQL_SQL_EXCEPTION, source, message);
    }
}
