
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.Mid;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;

public class MMid
{
    public static final TScalar INSTANCE = new Mid(MString.VARCHAR, MNumeric.INT);
}
