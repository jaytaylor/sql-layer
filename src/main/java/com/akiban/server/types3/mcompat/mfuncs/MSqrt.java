package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.Sqrt;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;

public class MSqrt {

    public static final TScalar INSTANCE = new Sqrt(MApproximateNumber.DOUBLE);
}
