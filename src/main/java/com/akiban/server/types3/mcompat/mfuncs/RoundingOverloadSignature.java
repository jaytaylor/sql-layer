
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.texpressions.TInputSetBuilder;

import java.util.List;

enum RoundingOverloadSignature {
    ONE_ARG {
        @Override
        protected int roundToScale(LazyList<? extends PValueSource> inputs) {
            return 0;
        }

        @Override
        protected void buildInputSets(TClass arg0, TInputSetBuilder builder) {
            builder.covers(arg0, 0);
        }

        @Override
        protected PValueSource getScaleOperand(List<? extends TPreptimeValue> inputs) {
            return ZERO;
        }

        private final PValueSource ZERO = new PValue(MNumeric.INT.instance(false), 0);
    },
    TWO_ARGS {
        @Override
        protected int roundToScale(LazyList<? extends PValueSource> inputs) {
            return inputs.get(1).getInt32();
        }

        @Override
        protected void buildInputSets(TClass arg0, TInputSetBuilder builder) {
            builder.covers(arg0, 0).covers(MNumeric.INT, 1);
        }

        @Override
        protected PValueSource getScaleOperand(List<? extends TPreptimeValue> inputs) {
            return inputs.get(1).value();
        }
    };

    protected abstract int roundToScale(LazyList<? extends PValueSource> inputs);
    protected abstract void buildInputSets(TClass arg0, TInputSetBuilder builder);
    protected abstract PValueSource getScaleOperand(List<? extends TPreptimeValue> inputs);
}
