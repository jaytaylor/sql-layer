
package com.akiban.server.error;

public class BadAISReferenceException extends InvalidOperationException {
    public BadAISReferenceException (String object, String objName, String reference, String refName) {
        super(ErrorCode.BAD_AIS_REFERENCE, object, objName, reference, refName);
    }
}
