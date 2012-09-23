/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.*;
import com.akiban.server.types3.common.BigDecimalWrapper;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimal;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import java.util.List;

public abstract class MRoundBase extends TOverloadBase {
    
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
    
    public static TOverload[] create(final RoundType roundType) {
        TOverload exactType = new MRoundBase(roundType, MNumeric.DECIMAL) {
                    
            private final int RET_TYPE_INDEX = 0;

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                TClass cached = (TClass) context.objectAt(RET_TYPE_INDEX);
                BigDecimalWrapper result = roundType.evaluate(MBigDecimal.getWrapper(inputs.get(0), context.inputTInstanceAt(0)));
                
                if (cached == null || cached == MNumeric.DECIMAL)
                    output.putObject(result);
                else if (cached == MNumeric.BIGINT)
                   output.putInt64(result.asBigDecimal().longValue());
                else // INT
                    output.putInt32(result.asBigDecimal().intValue());
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
                            return MNumeric.BIGINT.instance(ZERO_DEFAULT);
                        }
                        int length = precision - scale;
                        if (length >= 0 && length < 9) {
                            context.set(RET_TYPE_INDEX, MNumeric.INT);
                            return MNumeric.INT.instance(length + 3);
                        }
                        if (length >= 9 && length < 14) {
                            context.set(RET_TYPE_INDEX, MNumeric.BIGINT);
                            return MNumeric.BIGINT.instance(BIGINT_DEFAULT);
                        }

                        context.set(RET_TYPE_INDEX, MNumeric.DECIMAL);
                        return MNumeric.DECIMAL.instance(DECIMAL_DEFAULT, 0);
                    }
                });
            }
        };

        TOverload inexactType = new MRoundBase(roundType, MApproximateNumber.DOUBLE) {
            private int DEFAULT_DOUBLE = 17;
            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                double result = inputs.get(0).getDouble();
                output.putDouble(roundType.evaluate(result));
            }

            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.custom(numericType.instance(), new TCustomOverloadResult() {

                    @Override
                    public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                        return numericType.instance(DEFAULT_DOUBLE, 0);
                    }   
                });
            }
        };
        
        return new TOverload[]{exactType, inexactType};
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
