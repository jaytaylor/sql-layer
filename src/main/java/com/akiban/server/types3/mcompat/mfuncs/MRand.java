
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.Rand;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;

public class MRand {
    public static final TScalar[] INSTANCES = Rand.create(MNumeric.BIGINT, MApproximateNumber.DOUBLE);
}
