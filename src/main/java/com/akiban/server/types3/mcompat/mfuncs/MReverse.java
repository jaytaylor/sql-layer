
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.Reverse;
import com.akiban.server.types3.mcompat.mtypes.MString;

public class MReverse
{
    public static final TScalar INSTANCE = new Reverse(MString.VARCHAR);
}
