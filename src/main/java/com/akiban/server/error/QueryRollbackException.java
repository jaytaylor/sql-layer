
package com.akiban.server.error;

public class QueryRollbackException extends QueryCanceledException
{
    public QueryRollbackException()
    {
         super(ErrorCode.QUERY_ROLLBACK);
    }
}
