
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.Trim;
import com.akiban.server.types3.mcompat.mtypes.MString;

public class MTrim {
    public static final TScalar[] INSTANCES = Trim.create(MString.VARCHAR);
}
