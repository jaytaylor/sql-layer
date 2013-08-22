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
package com.foundationdb.server.types3.mcompat.mfuncs;

import com.foundationdb.server.types3.*;
import com.foundationdb.server.types3.common.BigDecimalWrapper;
import com.foundationdb.server.types3.mcompat.mtypes.MBigDecimal;
import com.foundationdb.server.types3.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types3.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;
import java.util.List;

public abstract class MRoundBase extends TScalarBase {

    static enum RoundType
    {
        CEIL() {
            @Override
            BigDecimalWrapper evaluate(BigDecimalWrapper result) {
                return result.ceil();
            }
            
            @Override
            double evaluate(double result) {
                return Math.ceil(result);
            }
        },
        FLOOR() {
            @Override
            BigDecimalWrapper evaluate(BigDecimalWrapper result) {
                return result.floor();
            }
            
            @Override
            double evaluate(double result) {
                return Math.floor(result);
            }
        };
        
        abstract double evaluate(double result);
        abstract BigDecimalWrapper evaluate(BigDecimalWrapper result);
    }
    
    public static TScalar[] create(final RoundType roundType) {
        TScalar exactType = new MRoundBase(roundType, MNumeric.DECIMAL) {
                    
            private static final int RET_TYPE_INDEX = 0;
            private static final int DEC_INDEX = 1;

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                TClass cached = (TClass) context.objectAt(RET_TYPE_INDEX);
                BigDecimalWrapper result = MBigDecimal.getWrapper(context, DEC_INDEX);
                result.set(MBigDecimal.getWrapper(inputs.get(0), context.inputTInstanceAt(0)));
                result = roundType.evaluate(result);
                
                TClass outT = context.outputTInstance().typeClass();
                if (cached != null && cached != outT)
                    throw new AssertionError(String.format("Mismatched type! cached: [%s] != output: [%s]",
                                                           cached,
                                                           outT));
                else if (outT == MNumeric.DECIMAL)
                    output.putObject(result);
                else if (outT == MNumeric.BIGINT)
                   output.putInt64(result.asBigDecimal().longValue());
                else if (outT == MNumeric.INT) // INT
                    output.putInt32(result.asBigDecimal().intValue());
                else
                    throw new AssertionError("Unexpected output type: " + outT);
            }

            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.custom(new TCustomOverloadResult() {

                    private final int ZERO_DEFAULT = 13;
                    private final int BIGINT_DEFAULT = 17;
                    private final int DECIMAL_DEFAULT = 16;

                    @Override
                    public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                        TPreptimeValue preptimeValue = inputs.get(0);
                        int precision = preptimeValue.instance().attribute(MBigDecimal.Attrs.PRECISION);
                        int scale = preptimeValue.instance().attribute(MBigDecimal.Attrs.SCALE);

                        // Special case: DECIMAL(0,0)
                        if (precision + scale == 0) {
                            context.set(RET_TYPE_INDEX, MNumeric.BIGINT);
                            return MNumeric.BIGINT.instance(ZERO_DEFAULT, preptimeValue.isNullable());
                        }
                        int length = precision - scale;
                        if (length >= 0 && length < 9) {
                            context.set(RET_TYPE_INDEX, MNumeric.INT);
                            return MNumeric.INT.instance(length + 3, preptimeValue.isNullable());
                        }
                        if (length >= 9 && length < 14) {
                            context.set(RET_TYPE_INDEX, MNumeric.BIGINT);
                            return MNumeric.BIGINT.instance(BIGINT_DEFAULT, preptimeValue.isNullable());
                        }

                        context.set(RET_TYPE_INDEX, MNumeric.DECIMAL);
                        return MNumeric.DECIMAL.instance(DECIMAL_DEFAULT, 0, preptimeValue.isNullable());
                    }
                });
            }
        };

        TScalar inexactType = new MRoundBase(roundType, MApproximateNumber.DOUBLE) {
            private int DEFAULT_DOUBLE = 17;
            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                double result = inputs.get(0).getDouble();
                output.putDouble(roundType.evaluate(result));
            }

            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.custom(new TInstanceGenerator(numericType), new TCustomOverloadResult() {

                    @Override
                    public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                        return numericType.instance(DEFAULT_DOUBLE, 0, anyContaminatingNulls(inputs));
                    }   
                });
            }
        };
        
        return new TScalar[]{exactType, inexactType};
    }
    protected final TClass numericType;
    private final RoundType roundType;

    MRoundBase(RoundType roundType, TClass numericType) {
        this.roundType = roundType;
        this.numericType = numericType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(numericType, 0);
    }

    @Override
    public String displayName() {
        return roundType.name();
    }
}
