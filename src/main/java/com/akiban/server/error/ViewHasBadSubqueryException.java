
package com.akiban.server.error;

public class ViewHasBadSubqueryException extends InvalidOperationException {
    public ViewHasBadSubqueryException (String viewName, String message) {
        super (ErrorCode.VIEW_BAD_SUBQUERY, viewName, message);
    }
}
