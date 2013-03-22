
package com.akiban.server.error;

import com.akiban.server.types.AkType;

public final class InconvertibleTypesException extends InvalidOperationException {
    public InconvertibleTypesException(AkType sourceType, AkType targetType) {
        super(ErrorCode.INCONVERTIBLE_TYPES, sourceType, targetType);
    }
}
