
package com.akiban.server.types3.aksql.akfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.common.funcs.IsNull;

public class AkIsNull
{
    public static final TScalar INSTANCE = IsNull.create(AkBool.INSTANCE);
}
