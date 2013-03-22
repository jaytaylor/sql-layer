
package com.akiban.server.error;

public class SubqueryOneColumnException extends InvalidOperationException {
    public SubqueryOneColumnException () {
        super(ErrorCode.SUBQUERY_ONE_COLUMN);
    }
}
