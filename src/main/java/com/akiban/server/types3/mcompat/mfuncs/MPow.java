
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.TPow;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;

public class MPow {
    public static final TScalar INSTANCE = new TPow(MApproximateNumber.DOUBLE) {};
}
