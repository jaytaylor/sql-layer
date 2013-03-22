
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.TLog;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;

public class MLogBase
{
    public static final TScalar INSTANCES[] = TLog.create(MApproximateNumber.DOUBLE);

    public static final TScalar TWO_ARG = new MLog(MApproximateNumber.DOUBLE);
}
