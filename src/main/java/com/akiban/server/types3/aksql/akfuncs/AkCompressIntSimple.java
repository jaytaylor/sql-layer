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

package com.akiban.server.types3.aksql.akfuncs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TCustomOverloadResult;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.aksql.aktypes.AkNumeric;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;

import java.util.List;

public final class AkCompressIntSimple extends TOverloadBase {
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(AkNumeric.BIGINT, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        PValueSource source = (PValueSource) context.objectAt(CONST_PREPTIME_INDEX);
        if (source == null)
            source = inputs.get(0);
        output.putInt64(source.getInt64());
    }

    @Override
    public String overloadName() {
        return "compressint-simple";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(
                AkNumeric.BIGINT.instance(),
                new TCustomOverloadResult() {
                    @Override
                    public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                        TPreptimeValue preptimeValue = inputs.get(0);

                        // if the preptimevalue is non-const, assume BIGINT
                        if (preptimeValue == null || preptimeValue.value() == null)
                            return AkNumeric.BIGINT.instance();

                        // const value. Find the smallest match
                        long value = preptimeValue.value().getInt64();

                        PValue constValue = new PValue(PUnderlying.INT_64);
                        constValue.putInt64(value);
                        context.set(CONST_PREPTIME_INDEX, constValue);

                        if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE)
                            return AkNumeric.SMALLINT.instance();
                        if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE)
                            return AkNumeric.INT.instance();
                        return AkNumeric.BIGINT.instance();
                    }
                });
    }

    private static final int CONST_PREPTIME_INDEX = 0;
}
