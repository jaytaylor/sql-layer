
package com.akiban.server.error;

public class WholeGroupQueryException extends InvalidOperationException {
    public WholeGroupQueryException () {
        super(ErrorCode.WHOLE_GROUP_QUERY);
    }
}
