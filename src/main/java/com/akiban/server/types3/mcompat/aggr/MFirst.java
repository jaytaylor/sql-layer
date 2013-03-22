
package com.akiban.server.types3.mcompat.aggr;

import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TAggregatorBase;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;

public class MFirst extends TAggregatorBase {

    public static final TAggregator[] INSTANCES = {
        new MFirst ("FIRST"),
    };
    
    protected MFirst(String name) {
        super(name, null);
    }

    @Override
    public void input(TInstance instance, PValueSource source,
            TInstance stateType, PValue state, Object option) {
        if (source.isNull())
            return;

        if (!state.hasAnyValue())
            PValueTargets.copyFrom(source, state);
    }

    @Override
    public void emptyValue(PValueTarget state) {
        state.putNull();
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.picking();
    }
}
