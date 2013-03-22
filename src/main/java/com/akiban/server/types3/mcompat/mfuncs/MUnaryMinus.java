
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TCustomOverloadResult;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.common.BigDecimalWrapper;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimal;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

import java.util.List;

public final class MUnaryMinus extends TScalarBase {

    private static final int DEC_INDEX = 0;

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(strategy.tClass, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        strategy.apply(inputs.get(0), output, context);
    }

    @Override
    public String displayName() {
        return "minus";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                return strategy.resultType(inputs.get(0).instance());
            }
        });
    }

    public static final TScalar[] overloads = create();

    private MUnaryMinus(Strategy strategy) {
        this.strategy = strategy;
    }

    private final Strategy strategy;

    private enum Strategy {
        INT(MNumeric.INT) {
            @Override
            protected void apply(PValueSource in, PValueTarget out, TExecutionContext context) {
                int neg = -in.getInt32();
                out.putInt32(neg);
            }

            @Override
            protected TInstance resultType(TInstance operand) {
                return MNumeric.INT.instance(operand.nullability());
            }
        },
        BIGINT(MNumeric.BIGINT) {
            @Override
            protected void apply(PValueSource in, PValueTarget out, TExecutionContext context) {
                long neg = -in.getInt64();
                out.putInt64(neg);
            }

            @Override
            protected TInstance resultType(TInstance operand) {
                return MNumeric.BIGINT.instance(operand.nullability());
            }
        },
        DOUBLE(MApproximateNumber.DOUBLE) {
            @Override
            protected void apply(PValueSource in, PValueTarget out, TExecutionContext context) {
                double neg = -in.getDouble();
                out.putDouble(neg);
            }

            @Override
            protected TInstance resultType(TInstance operand) {
                return MApproximateNumber.DOUBLE.instance(operand.nullability());
            }
        },
        DECIMAL(MNumeric.DECIMAL) {
            @Override
            protected void apply(PValueSource in, PValueTarget out, TExecutionContext context) {
                BigDecimalWrapper wrapped = MBigDecimal.getWrapper(context, DEC_INDEX);
                wrapped.set(MBigDecimal.getWrapper(in, context.inputTInstanceAt(0)));
                wrapped.negate();
                out.putObject(wrapped);
            }

            @Override
            protected TInstance resultType(TInstance operand) {
                return operand;
            }
        }
        ;
        protected abstract void apply(PValueSource in, PValueTarget out, TExecutionContext context);
        protected abstract TInstance resultType(TInstance operand);

        private Strategy(TClass tClass) {
            this.tClass = tClass;
        }

        private final TClass tClass;
    }

    private static TScalar[] create() {
        Strategy[] strategies = Strategy.values();
        TScalar[] results = new TScalar[strategies.length];
        for (int i = 0, strategiesLength = strategies.length; i < strategiesLength; i++) {
            Strategy strategy = strategies[i];
            results[i] = new MUnaryMinus(strategy);
        }
        return results;
    }

}
