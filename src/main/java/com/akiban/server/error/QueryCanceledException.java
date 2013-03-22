
package com.akiban.server.error;

import com.akiban.server.service.session.Session;

public class QueryCanceledException extends InvalidOperationException
{
    public QueryCanceledException(Session session)
    {
        this(ErrorCode.QUERY_CANCELED);
        // Clear state causing current query to terminate ...
        session.cancelCurrentQuery(false); // in the session
        Thread.interrupted(); // and the thread's interruption flag
    }

    protected QueryCanceledException(ErrorCode errorCode, Object... args)
    {
        super(errorCode, args);
    }
}
