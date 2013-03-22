
package com.akiban.server.error;

public class QueryTimedOutException extends QueryCanceledException
{
    public QueryTimedOutException(long executionTimeMsec)
    {
         super(ErrorCode.QUERY_TIMEOUT, executionTimeMsec);
    }
}
