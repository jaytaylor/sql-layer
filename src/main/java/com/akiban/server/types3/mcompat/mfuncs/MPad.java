
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.Pad;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;

public class MPad
{
    public static final TScalar[] INSTANCES
            = Pad.create(MString.VARCHAR, MNumeric.INT);
}
