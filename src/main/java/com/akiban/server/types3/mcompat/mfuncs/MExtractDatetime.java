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

public abstract class MExtractDatetime extends TOverloadBase {

    private final TClass dateType;
    
    public static final TOverload YEAR = new MExtractDatetime(MDatetimes.DATE) {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int date = inputs.get(0).getInt32();
            output.putInt32(DateExtractor.getDate(date)[0]);
        }

        @Override
        public String overloadName() {
            return "YEAR";
        }
    };
    
    public static final TOverload MONTH = new MExtractDatetime(MDatetimes.DATE) {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int date = inputs.get(0).getInt32();
            output.putInt32(DateExtractor.getDate(date)[1]);
        }

        @Override
        public String overloadName() {
            return "MONTH";
        } 
    };
        
    public static final TOverload QUARTER = new MExtractDatetime(MDatetimes.DATE) {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int date = inputs.get(0).getInt32();
            int month = DateExtractor.getDate(date)[1];
            
            int result;
            if (month < 4) result = 1;
            else if (month < 7) result = 2;
            else if (month < 10) result = 3;
            else result = 4;
            
            output.putInt32(result);
        }

        @Override
        public String overloadName() {
            return "QUARTER";
        }    
    };
    
    public static final TOverload DAY = new MExtractDatetime(MDatetimes.DATE) {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int date = inputs.get(0).getInt32();
            output.putInt32(DateExtractor.getDate(date)[2]);
        }

        @Override
        public String overloadName() {
            return "DAY";
        }       
    }; 
        
    public static final TOverload HOUR = new MExtractDatetime(MDatetimes.DATETIME) {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long datetime = inputs.get(0).getInt64();
            
            output.putInt64(DateExtractor.getDatetime(datetime)[3]);
        }

        @Override
        public String overloadName() {
            return "HOUR";
        }
    };
    
    public static final TOverload MINUTE = new MExtractDatetime(MDatetimes.DATETIME) {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long datetime = inputs.get(0).getInt64();
            
            output.putInt64(DateExtractor.getDatetime(datetime)[4]);
        }

        @Override
        public String overloadName() {
            return "MINUTE";
        }
    };
    
    public static final TOverload SECOND = new MExtractDatetime(MDatetimes.DATETIME) {

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            long datetime = inputs.get(0).getInt64();
            
            output.putInt64(DateExtractor.getDatetime(datetime)[5]);        }

        @Override
        public String overloadName() {
            return "SECOND";
        }
    };

    private MExtractDatetime(TClass dateType) {
        this.dateType = dateType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(dateType, 0);
    }
    
    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(dateType.instance(), new TCustomOverloadResult() {

            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                return MNumeric.INT.instance();
            }
        });
    }
}
