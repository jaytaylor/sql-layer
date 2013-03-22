
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.InetAton;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;

public class MInetAton
{
    public static final TScalar INSTANCE = new InetAton(MString.VARCHAR, MNumeric.BIGINT);
}
