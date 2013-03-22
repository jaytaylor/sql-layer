
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.UpperLower;
import com.akiban.server.types3.mcompat.mtypes.MString;

public class MUpperLower
{
    public static final TScalar INSTANCES[] = UpperLower.create(MString.VARCHAR);
}
