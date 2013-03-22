
package com.akiban.server.error;

public class BadStatisticsTypeException extends InvalidOperationException {
    public BadStatisticsTypeException (int type) {
        super (ErrorCode.BAD_STATISTICS_TYPE, type);
    }
}
