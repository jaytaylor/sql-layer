
package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.*;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public abstract class TArithmetic extends TScalarBase {

    protected TArithmetic(String overloadName, TClass operand0, TClass operand1, TClass resultType, int... attrs) {
       this.overloadName = overloadName;
       this.operand0 = operand0;
       this.operand1 = operand1;
       this.resultType = resultType;
       this.attrs = attrs;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        TInstanceNormalizer normalizer = inputSetInstanceNormalizer();
        if (operand0 == operand1)
            builder.nextInputPicksWith(normalizer).covers(operand0, 0, 1);
        else {
            builder.nextInputPicksWith(normalizer).covers(operand0, 0);
            builder.nextInputPicksWith(normalizer).covers(operand1, 1);
        }
    }

    @Override
    public String displayName() {
        return overloadName;
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(resultType, attrs);
    }

    protected TInstanceNormalizer inputSetInstanceNormalizer() {
        return null;
    }

    private final String overloadName;
    private final TClass operand0;
    private final TClass operand1;
    private final TClass resultType;
    private final int[] attrs;
}
