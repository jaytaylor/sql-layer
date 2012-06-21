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
import com.akiban.server.types3.common.DateExtractor;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import java.util.List;
import org.joda.time.MutableDateTime;

public abstract class MDatetimeArith extends TOverloadBase {
    
    private final String name;
    
    public static TOverload DATE_ADD = new MDatetimeArith("DATE_ADD") {

        @Override
        protected long evaluate(long input, MutableDateTime datetime) {
            datetime.add(input);
        }
    };
    
    public static TOverload DATE_SUB = new MDatetimeArith("DATE_SUB") {

        @Override
        protected long evaluate(long input, MutableDateTime datetime) {
            datetime.add(-input);
        }
    };
    
    public static TOverload SUBTIME = new MDatetimeArith("SUBTIME") {

        @Override
        protected long evaluate(long input, MutableDateTime datetime) {
            datetime.add(-input);
        }
    };
    
    public static TOverload ADDTIME = new MDatetimeArith("ADDTIME") {

        @Override
        protected long evaluate(long input, MutableDateTime datetime) {
            datetime.add(input);
        }
    };
    
    public static TOverload DATEDIFF = new MDatetimeArith("DATEDIFF") {

        @Override
        protected long evaluate(long input, MutableDateTime datetime) {
            datetime.add(-input);
            return datetime.getMillis() / DateExtractor.DAY_FACTOR;
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.custom(MDatetimes.DATE.instance(), new TCustomOverloadResult() {

                @Override
                public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                    return MNumeric.INT.instance();
                }
            });
        }
    };
    
    public static TOverload TIMEDIFF = new MDatetimeArith("TIMEDIFF") {

        @Override
        protected long evaluate(long input, MutableDateTime datetime) {
            datetime.add(-input);
        }
    };
    
    private MDatetimeArith(String name) {
        this.name = name;
    }
    
    protected abstract long evaluate(long input, MutableDateTime datetime);
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        long[] arr1 = DateExtractor.extract(inputs.get(1).getInt64());
        MutableDateTime datetime = DateExtractor.getMutableDateTime(context, arr1, true);
        long value = datetime.getMillis();

        // Reuse MutableDateTime object to save additional allocation
        long[] arr0 = DateExtractor.extract(inputs.get(0).getInt64());
        datetime.setDateTime((int) arr0[DateExtractor.YEAR], (int) arr0[DateExtractor.MONTH], (int) arr0[DateExtractor.DAY],
                (int) arr0[DateExtractor.HOUR], (int) arr0[DateExtractor.MINUTE], (int) arr0[DateExtractor.SECOND], 0);
        
        //long date = DateExtractor.toDatetime(datetime.getYear(), datetime.getMonthOfYear(), datetime.getDayOfMonth(),
        //        datetime.getHourOfDay(), datetime.getMinuteOfHour(), datetime.getSecondOfMinute());
        output.putInt64(evaluate(value, datetime));
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MDatetimes.DATETIME, 0, 1);
    }
        
    @Override
    public String overloadName() {
        return name;
    }
    
    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(MDatetimes.DATETIME.instance(), new TCustomOverloadResult() {

            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                TClass tClass = inputs.get(0).instance().typeClass();
                // RETURN LOGIC HERE.
            }
        }); 
    }
}
