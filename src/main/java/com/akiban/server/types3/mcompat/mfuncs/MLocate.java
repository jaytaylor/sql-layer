
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.Locate;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;

public class MLocate
{
    public static final TScalar LOCATE
            = Locate.create3ArgOverload(MString.VARCHAR, MNumeric.INT, "LOCATE");
    
    public static final TScalar LOCATE_2
            = Locate.create2ArgOverload(MString.VARCHAR, MNumeric.INT, "LOCATE");
    
    public static final TScalar POSITION
            = Locate.create3ArgOverload(MString.VARCHAR, MNumeric.INT, "POSITION");
}
