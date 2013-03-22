
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.TLike;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.service.Scalar;

public class MLike
{
    @Scalar
    public static final TScalar[] LIKE_OVERLOADS = TLike.create(MString.VARCHAR);
}
