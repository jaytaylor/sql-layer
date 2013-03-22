
package com.akiban.server.error;

public class TapBeanFailureException extends InvalidOperationException {
    public TapBeanFailureException (String failure) {
        super (ErrorCode.TAP_BEAN_FAIL, failure);
    }
}
