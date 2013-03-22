
package com.akiban.server.types3.mcompat.aggr;

import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TFixedTypeAggregator;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;

public abstract class MBit extends TFixedTypeAggregator {
    
    public static final TAggregator[] INSTANCES = {
        // BIT_AND
        new MBit("BIT_AND") {

            @Override
            long process(long i0, long i1) {
                return i0 & i1;
            }
        }, 
        // BIT_OR
        new MBit("BIT_OR") {

            @Override
            long process(long i0, long i1) {
                return i0 | i1;
            }
        }, 
        // BIT_XOR
        new MBit("BIT_XOR") {

            @Override
            long process(long i0, long i1) {
                return i0 ^ i1;
            }
        }
    };
    
    abstract long process(long i0, long i1);

    @Override
    public void input(TInstance instance, PValueSource source, TInstance stateType, PValue state, Object o) {
        if (!source.isNull()) {
            long incoming = source.getInt64();
            if (!state.hasAnyValue()) {
                state.putInt64(incoming);
            }
            else {
                long previousState = state.getInt64();
                state.putInt64(process(previousState, incoming));
            }
        }    
    }

    @Override
    public void emptyValue(PValueTarget state) {
        state.putNull();
    }

    private MBit(String name) {
        super(name, MNumeric.BIGINT);
    }
}
