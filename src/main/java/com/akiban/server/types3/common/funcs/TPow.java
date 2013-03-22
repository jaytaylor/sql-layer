
package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.*;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public class TPow extends TScalarBase {
    
    private final TClass inputType;
    
    protected TPow(TClass inputType) {
        this.inputType = inputType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(inputType, 0, 1);
    }
        
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        double a0 = inputs.get(0).getDouble();
        double a1 = inputs.get(1).getDouble();
        output.putDouble(Math.pow(a0, a1));
    }
        
    @Override
    public String displayName() {
        return "POW";
    }

    @Override
    public String[] registeredNames()
    {
        return new String[]{"pow", "power"};
    }
    
    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(inputType);
    }
}
