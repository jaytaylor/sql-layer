
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.Substring;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;

public class MSubstring {
    public static final TScalar INSTANCES[] = Substring.create(MString.VARCHAR, MNumeric.INT);
}
