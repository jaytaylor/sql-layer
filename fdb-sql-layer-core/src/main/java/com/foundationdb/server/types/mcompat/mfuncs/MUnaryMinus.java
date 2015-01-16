/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.util.List;

public final class MUnaryMinus extends TScalarBase {

    private static final int DEC_INDEX = 0;

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(strategy.tClass, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
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
                return strategy.resultType(inputs.get(0).type());
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
            protected void apply(ValueSource in, ValueTarget out, TExecutionContext context) {
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
            protected void apply(ValueSource in, ValueTarget out, TExecutionContext context) {
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
            protected void apply(ValueSource in, ValueTarget out, TExecutionContext context) {
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
            protected void apply(ValueSource in, ValueTarget out, TExecutionContext context) {
                BigDecimalWrapper wrapped = TBigDecimal.getWrapper(context, DEC_INDEX);
                wrapped.set(TBigDecimal.getWrapper(in, in.getType()));
                wrapped.negate();
                out.putObject(wrapped);
            }

            @Override
            protected TInstance resultType(TInstance operand) {
                return operand;
            }
        }
        ;
        protected abstract void apply(ValueSource in, ValueTarget out, TExecutionContext context);
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
