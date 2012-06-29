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

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import org.joda.time.MutableDateTime;




public class MTimeAndDate extends TOverloadBase{
    
     static enum OutputType {
        DATE {
            @Override
            long evaluate(long[] input)
            {
                return MDatetimes.encodeDate(new long[]{input[MDatetimes.YEAR_INDEX],input[MDatetimes.MONTH_INDEX],input[MDatetimes.DAY_INDEX]});
            }
            
            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.fixed(MDatetimes.DATE.instance());
            }
        },
        TIME {
            @Override
            long evaluate(long[] input)
            {
                return MDatetimes.encodeDate(new long[]{0,0,0,input[MDatetimes.HOUR_INDEX],input[MDatetimes.MIN_INDEX],input[MDatetimes.SEC_INDEX]});
            }
            
            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.fixed(MDatetimes.TIME.instance());
            }
        };
        
        abstract public TOverloadResult resultType();
        abstract long evaluate(long[] input);
    }
    
    private final OutputType outputType;
    
    MTimeAndDate(OutputType outputType) {
        this.outputType = outputType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MDatetimes.DATETIME, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        long[] datetime = MDatetimes.decodeDatetime(inputs.get(0).getInt64());
        
        if (!MDatetimes.isValidDatetime(datetime)) output.putNull();
        else output.putInt32((int)(outputType.evaluate(datetime)));
    }

    @Override
    public String overloadName() {
        return outputType.name();
    }

    @Override
    public TOverloadResult resultType() {
        return outputType.resultType();
    }
}
