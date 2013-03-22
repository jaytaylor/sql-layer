
package com.akiban.server.types3.texpressions;

import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public final class TValidatedAggregator extends TValidatedOverload implements TAggregator {
    @Override
    public void input(TInstance instance, PValueSource source, TInstance stateType, PValue state, Object option) {
        aggregator.input(instance, source, stateType, state, option);
    }

    @Override
    public void emptyValue(PValueTarget state) {
        aggregator.emptyValue(state);
    }

    public TValidatedAggregator(TAggregator overload) {
        super(overload);
        this.aggregator = overload;
    }
    
    private final TAggregator aggregator;
}
