
package com.akiban.server.types3.pvalue;

import com.akiban.server.types.AkType;

public enum PUnderlying {
    BOOL, INT_8, INT_16, UINT_16, INT_32, INT_64, FLOAT, DOUBLE, BYTES, STRING
    ;

    public static PUnderlying valueOf(AkType akType) {
        switch (akType) {
        case INT:
            return PUnderlying.INT_64;
        case VARCHAR:
            return PUnderlying.BYTES;
        default:
            throw new AssertionError(akType);
        }
    }
}
