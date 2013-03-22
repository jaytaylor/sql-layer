
package com.akiban.server.types3.mcompat.aggr;

import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TAggregatorBase;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;

public class MMinMaxAggregation extends TAggregatorBase {

    private final MType mType;
    
    private enum MType {
        MIN() {
            @Override
            boolean condition(int a) {
                return a < 0;
            }   
        }, 
        MAX() {
            @Override
            boolean condition(int a) {
                return a > 0;
            }
        };
        abstract boolean condition (int a);
    }

    public static final TAggregator MIN = new MMinMaxAggregation(MType.MIN);
    public static final TAggregator MAX = new MMinMaxAggregation(MType.MAX);
    
    private MMinMaxAggregation(MType mType) {
        super(mType.name(), null);
        this.mType = mType;
    }

    @Override
    public void input(TInstance instance, PValueSource source, TInstance stateType, PValue state, Object o) {
        if (source.isNull())
            return;
        if (!state.hasAnyValue()) {
            PValueTargets.copyFrom(source, state);
            return;
        }
        TClass tClass = instance.typeClass();
        assert stateType.typeClass().equals(tClass) : "incompatible types " + instance + " and " + stateType;
        int comparison = TClass.compare(instance, source, stateType, state);
        if (mType.condition(comparison))
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
