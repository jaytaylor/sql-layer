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
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
import java.util.List;
import org.joda.time.MutableDateTime;

public abstract class MDatetimeArith extends TScalarBase {
    
    private final String name;
    private static final long DAY_FACTOR = 3600L * 1000 * 24;

    public static final TScalar DATEDIFF = new MDatetimeArith("DATEDIFF") {

        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            buildOneTypeInputSets(builder);
        }
                
        @Override
        protected void evaluate(long input, MutableDateTime datetime, PValueTarget output) {
            datetime.add(-input);
            output.putInt64((int) (datetime.getMillis() / DAY_FACTOR));
        }

        @Override
        public TOverloadResult resultType() {
            return intResultType();
        }
    };
    
    public static final TScalar TIMEDIFF = new MDatetimeArith("TIMEDIFF") {

        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            buildOneTypeInputSets(builder);
        }
        
        @Override
        protected void evaluate(long input, MutableDateTime datetime, PValueTarget output) {
            datetime.add(-input);
            long[] val = MDatetimes.fromJodaDatetime(datetime);
            val[0] = 0;
            val[1] = 0;
            val[2] = 0;
            long time = MDatetimes.encodeDatetime(val);
            output.putInt64(time);
        }
        
        @Override
        public TOverloadResult resultType() {
            return timeResultType();
        }
    };
    
    private MDatetimeArith(String name) {
        this.name = name;
    }
    
    protected abstract void evaluate(long input, MutableDateTime datetime, PValueTarget output);
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        long[] arr1 = MDatetimes.decodeDatetime(inputs.get(1).getInt64());
        MutableDateTime datetime = MDatetimes.toJodaDatetime(arr1, context.getCurrentTimezone());
        long value = datetime.getMillis();

        // Reuse MutableDateTime object to save additional allocation
        long[] arr0 = MDatetimes.decodeDatetime(inputs.get(0).getInt64());
        datetime.setDateTime((int) arr0[MDatetimes.YEAR_INDEX], (int) arr0[MDatetimes.MONTH_INDEX], (int) arr0[MDatetimes.DAY_INDEX],
                (int) arr0[MDatetimes.HOUR_INDEX], (int) arr0[MDatetimes.MIN_INDEX], (int) arr0[MDatetimes.SEC_INDEX], 0);
        evaluate(value, datetime, output);
    }
    
    protected void buildTwoTypeInputSets(TInputSetBuilder builder) {
        builder.covers(MDatetimes.DATETIME, 0, 1);
        builder.covers(MNumeric.INT, 2);
    }
    
    protected void buildOneTypeInputSets(TInputSetBuilder builder) {
        builder.covers(MDatetimes.DATETIME, 0, 1);
    }
        
    @Override
    public String displayName() {
        return name;
    }
    
    public TOverloadResult dateResultType() {
        return TOverloadResult.fixed(MDatetimes.DATETIME);
    }
    
    public TOverloadResult intResultType() {
        return TOverloadResult.custom(MDatetimes.DATETIME.instance(), new TCustomOverloadResult() {

            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                return MNumeric.INT.instance();
            }
        });
    }

    public TOverloadResult timeResultType() {
        return TOverloadResult.custom(MDatetimes.DATETIME.instance(), new TCustomOverloadResult() {

            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                return MDatetimes.TIME.instance();
            }
        });
    }
}
