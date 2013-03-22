
package com.akiban.server.error;

public class SelectExistsErrorException extends InvalidOperationException {
    public SelectExistsErrorException () {
        super (ErrorCode.SELECT_EXISTS_ERROR);
    }
}
