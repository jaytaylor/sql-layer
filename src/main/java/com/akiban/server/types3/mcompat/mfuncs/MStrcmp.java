
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public class MStrcmp extends TScalarBase {
    public static final TScalar INSTANCE = new MStrcmp();
    
    private MStrcmp(){}
            
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MString.VARCHAR, 0, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        String st1 = inputs.get(0).getString();
        String st2 = inputs.get(1).getString();

        int cmp = st1.compareTo(st2);
        if (cmp < 0)
            cmp = -1;
        else if (cmp > 0)
            cmp = 1;
        output.putInt32(cmp);
    }

    @Override
    public String displayName() {
        return "STRCMP";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.INT);
    }

}
