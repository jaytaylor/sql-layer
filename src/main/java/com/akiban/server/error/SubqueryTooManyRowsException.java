
package com.akiban.server.error;

public class SubqueryTooManyRowsException extends InvalidOperationException {
    public SubqueryTooManyRowsException () {
        super(ErrorCode.SUBQUERY_TOO_MANY_ROWS);
    }
}
