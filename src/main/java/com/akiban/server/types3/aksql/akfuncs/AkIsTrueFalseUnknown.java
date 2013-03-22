
package com.akiban.server.types3.aksql.akfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.common.funcs.IsTrueFalseUnknown;

public class AkIsTrueFalseUnknown
{
    public static final TScalar INSTANCES[] = IsTrueFalseUnknown.create(AkBool.INSTANCE);
}
