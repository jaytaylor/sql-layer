
package com.akiban.server.types3.mcompat.aggr;

import com.akiban.server.error.OverflowException;
import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TFixedTypeAggregator;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.common.BigDecimalWrapper;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimal;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.pvalue.PValueTargets;

public class MSum extends TFixedTypeAggregator {

    private final SumType sumType;
    
    private enum SumType {
        BIGINT(MNumeric.BIGINT) {
            @Override
            void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                long oldState = source.getInt64();
                long input = state.getInt64();
                long sum = oldState + input;
                if (oldState > 0 && input > 0 && sum <= 0) {
                    throw new OverflowException();
                } else if (oldState < 0 && input < 0 && sum >= 0) {
                    throw new OverflowException();
                } else {
                    state.putInt64(sum);
                }
            }
        }, 
        DOUBLE(MApproximateNumber.DOUBLE) {
            @Override
            void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                double oldState = source.getDouble();
                double input = state.getDouble();
                double sum = oldState + input;
                if (Double.isInfinite(sum) && !Double.isInfinite(oldState) && !Double.isInfinite(input)) {
                    throw new OverflowException();
                } else {
                    state.putDouble(sum);
                }
            }
        },
        DECIMAL(MNumeric.DECIMAL) {
            @Override
            void input(TInstance instance, PValueSource source, TInstance stateType, PValue state) {
                BigDecimalWrapper oldState = MBigDecimal.getWrapper(source, instance);
                BigDecimalWrapper input = MBigDecimal.getWrapper(state, instance);
                state.putObject(oldState.add(input));
            }
        }
        ;
        abstract void input(TInstance instance, PValueSource source, TInstance stateType, PValue state);
        private final TClass typeClass;
        
        private SumType(TClass typeClass) {
            this.typeClass = typeClass;
        }
    }
    
    public static final TAggregator[] INSTANCES = {
        new MSum(SumType.DECIMAL),
        new MSum(SumType.DOUBLE),
        new MSum(SumType.BIGINT)
    };
    
    private MSum(SumType sumType) {
        super("sum", sumType.typeClass);
        this.sumType = sumType;
    }
    
    @Override
    public void input(TInstance instance, PValueSource source, TInstance stateType, PValue state, Object o) {
            if (source.isNull())
                return;
        if (!state.hasAnyValue())
            PValueTargets.copyFrom(source, state);
        else
            sumType.input(instance, source, stateType, state);
    }

    @Override
    public void emptyValue(PValueTarget state) {
        state.putNull();
    }
}
