
package com.akiban.server.error;

import com.akiban.ais.model.Type;


public class UnknownTypeSizeException extends InvalidOperationException {
    public UnknownTypeSizeException (Type aisType) {
        super(ErrorCode.UNKNOWN_TYPE_SIZE, aisType.toString());
    }
}
