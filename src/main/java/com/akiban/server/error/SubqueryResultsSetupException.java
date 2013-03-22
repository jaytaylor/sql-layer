
package com.akiban.server.error;

public class SubqueryResultsSetupException extends InvalidOperationException {
    public SubqueryResultsSetupException (String message) {
        super (ErrorCode.SUBQUERY_RESULT_FAIL, message);
    }
}
