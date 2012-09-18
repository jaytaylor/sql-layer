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
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import java.util.List;

public abstract class M2ArgRoundBase extends TOverloadBase {

    public static final M2ArgRoundBase ROUND = new M2ArgRoundBase() {
        @Override
        protected void doRounding(BigDecimalWrapper io, int scale) {
            io.truncate(scale);
        }

        @Override
        public String displayName() {
            return "ROUND";
        }
    };

    public static final M2ArgRoundBase TRUNCATE = new M2ArgRoundBase() {
        @Override
        protected void doRounding(BigDecimalWrapper io, int scale) {
            io.truncate(scale);
        }

        @Override
        public String displayName() {
            return "TRUNCATE";
        }
    };

    protected abstract void doRounding(BigDecimalWrapper io, int scale);

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        BigDecimalWrapper result = MBigDecimal.getWrapper(inputs.get(0), context.inputTInstanceAt(0));
        int scale = (int) Math.round(inputs.get(1).getDouble());
        doRounding(result, scale);
        output.putObject(result);
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                TPreptimeValue valueToRound = inputs.get(0);
                PValueSource roundToPVal = inputs.get(1).value();
                int precision, scale;
                if (roundToPVal == null) {
                    precision = 17;
                    int incomingScale = valueToRound.instance().attribute(MBigDecimal.Attrs.SCALE);
                    if (incomingScale > 1)
                        precision += (incomingScale - 1);
                    scale = incomingScale;
                }
                else {
                    scale = roundToPVal.getInt32();

                    TInstance incomingInstance = valueToRound.instance();
                    int incomingPrecision = incomingInstance.attribute(MBigDecimal.Attrs.PRECISION);
                    int incomingScale = incomingInstance.attribute(MBigDecimal.Attrs.SCALE);

                    precision = incomingPrecision;
                    if (incomingScale > 1)
                        precision -= (incomingScale - 1);
                    precision += scale;

                }
                return MNumeric.DECIMAL.instance(precision, scale);
            }
        });
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MNumeric.DECIMAL, 0).covers(MNumeric.INT, 1);
    }

    private M2ArgRoundBase() {}
}