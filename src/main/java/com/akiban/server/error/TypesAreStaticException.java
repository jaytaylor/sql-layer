
package com.akiban.server.error;

import com.akiban.ais.model.Type;

public class TypesAreStaticException extends InvalidOperationException {
    public TypesAreStaticException (Type type) {
        super (ErrorCode.TYPES_ARE_STATIC, type);
    }
}
