
package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public class Sqrt extends TScalarBase {

    private final TClass type; 
    
    public Sqrt(TClass type) {
        this.type = type;
    }
      
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(type, 0);
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        double value = inputs.get(0).getDouble();
        if (value < 0) {
            output.putNull();
        } else {
            output.putDouble(Math.sqrt(value));
        }
    }
      
    @Override
    public String displayName() {
        return "SQRT";
    }
    
    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(type);
    }

}
