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

import com.akiban.server.error.OutOfRangeException;
import com.akiban.server.types3.*;
import com.akiban.server.types3.common.BigDecimalWrapper;
import com.akiban.server.types3.mcompat.mtypes.MBigDecimal;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import java.util.List;

public class M2ArgRoundBase extends TOverloadBase {

    M2ArgRoundBase(RoundType roundType, TClass numericType, Attribute precisionAttr, Attribute scaleAttr) {
        this.roundType = roundType;
        this.numericType = numericType;
        this.precisionAttr = precisionAttr;
        this.scaleAttr = scaleAttr;
    }

    static enum RoundType {

        TRUNCATE() {

            @Override
            BigDecimalWrapper evaluate(BigDecimalWrapper result, int scale) {
                return result.truncate(scale);
            }
        },
        ROUND() {

            @Override
            BigDecimalWrapper evaluate(BigDecimalWrapper result, int scale) {
                return result.round(scale);
            }
        };

        abstract BigDecimalWrapper evaluate(BigDecimalWrapper result, int scale);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        BigDecimalWrapper result = MBigDecimal.getWrapper(inputs.get(0), context.inputTInstanceAt(0));
        int scale = (int) Math.round(inputs.get(1).getDouble());
        output.putObject(roundType.evaluate(result, scale));
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(numericType.instance(), new TCustomOverloadResult() {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                TPreptimeValue preptimeValue = inputs.get(0);
                int precision = preptimeValue.instance().attribute(precisionAttr);
                int scale = preptimeValue.instance().attribute(scaleAttr);

                PValueSource roundToVal = inputs.get(1).value();
                if (roundToVal != null) {
                    int leftOfDecimal = precision - scale;
                    long roundToIntL = roundToVal.getInt64();
                    int roundToInt = (int) roundToIntL;
                    if (roundToInt != roundToIntL)
                        throw new OutOfRangeException(roundToIntL + " not an int");
                    if (roundToInt < 0) {
                        precision = leftOfDecimal;
                        scale = 0;
                    } else {
                        precision = leftOfDecimal + roundToInt;
                        scale = 0;
                    }
                }
                return MNumeric.DECIMAL.instance(precision, scale);
            }
        });
    }

    private final TClass numericType;
    private final RoundType roundType;
    private final Attribute precisionAttr;
    private final Attribute scaleAttr;

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(numericType, 0).covers(MNumeric.BIGINT, 1);
    }

    @Override
    public String displayName() {
        return roundType.name();
    }
}