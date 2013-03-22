
package com.akiban.server.error;

public class AISNullReferenceException extends InvalidOperationException {
    public AISNullReferenceException (String object, String name, String reference) {
        super(ErrorCode.NULL_REFERENCE, object, name, reference);
    }
}
