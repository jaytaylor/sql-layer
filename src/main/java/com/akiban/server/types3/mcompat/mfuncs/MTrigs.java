
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.TTrigs;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;

public class MTrigs
{
    // This is a 'strange' case
    //
    // in aksql, the return type would always be AkNumeric.DOUBLE.instance()
    //
    // but in mysql, there could be multiple instances of the same TClass
    // (each differing from each other by the width)
    // So we'd define a fixed/default width that this function returns
    
    public static final TScalar TRIGS[] = TTrigs.create(MApproximateNumber.DOUBLE);
}
