
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.LeftRight;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;

public class MLeftRight
{
    public static final TScalar LEFT = LeftRight.getLeft(MString.VARCHAR, MNumeric.INT);
    
    public static final TScalar RIGHT = LeftRight.getRight(MString.VARCHAR, MNumeric.INT);
}
