
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public abstract class MMinMaxScalar extends TScalarBase {
    public static final TScalar INSTANCES[] = new TScalar[]
    {
        new MMinMaxScalar ("_min") {
            @Override
            protected int getIndex(int comparison) {
                return comparison < 0 ? 0 : 1;
            }
        },
        new MMinMaxScalar ("_max") {
            @Override
            protected int getIndex(int comparison) {
                return comparison > 0 ? 0 : 1;
            }
        },
    };

    private final String name;
    
    private MMinMaxScalar (String name) {
        this.name = name;
    }

    @Override
    public String displayName() {
        return name;
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.picking();
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.pickingCovers(null, 0, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context,
            LazyList<? extends PValueSource> inputs, PValueTarget output) {
        int comparison = TClass.compare(inputs.get(0).tInstance(), inputs.get(0), inputs.get(1).tInstance(), inputs.get(1));
        int index = getIndex (comparison);
        PValueTargets.copyFrom(inputs.get(index), output);
    }
    
    protected abstract int getIndex(int comparison);
}
