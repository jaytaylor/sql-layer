
package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.server.types3.texpressions.Constantness;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public class Coalesce extends TScalarBase {

    public static final TScalar INSTANCE = new Coalesce();

    private Coalesce() {}
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.pickingVararg(null, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        for (int i = 0; i < inputs.size(); ++i) {
            if (!inputs.get(i).isNull()) {
                PValueTargets.copyFrom(inputs.get(i), output);
                return;
            }
        }
        output.putNull();
    }

    @Override
    public String displayName() {
        return "COALESCE";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.picking();
    }

    @Override
    protected boolean nullContaminates(int inputIndex) {
        return false;
    }

    @Override
    protected Constantness constness(int inputIndex, LazyList<? extends TPreptimeValue> values) {
        PValueSource preptimeValue = constSource(values, inputIndex);
        if (preptimeValue == null)
            return Constantness.NOT_CONST;
        return preptimeValue.isNull() ? Constantness.UNKNOWN : Constantness.CONST;
    }
}
