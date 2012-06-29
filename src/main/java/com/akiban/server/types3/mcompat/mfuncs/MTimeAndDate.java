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
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;

public class MTimeAndDate extends TOverloadBase{
   
    static enum DateType {
        DATETIME(MDatetimes.DATETIME) {
            @Override
            boolean isValid(long[] datetime) {
                return MDatetimes.isValidDatetime(datetime);
            }
            
            @Override
            long[] decode(long input) {
                return MDatetimes.decodeDatetime(input);
            }
        },
        DATE(MDatetimes.DATE) {
            @Override
            boolean isValid(long[] datetime) {
                return MDatetimes.isValidDayMonth(datetime);
            }      
            
            @Override
            long[] decode(long input) {
                return MDatetimes.decodeDate(input);
            }
        },
        TIME(MDatetimes.TIME) {
            @Override
            boolean isValid(long[] datetime) {
                return MDatetimes.isValidHrMinSec(datetime);
            }            
            
            @Override
            long[] decode(long input) {
                return MDatetimes.decodeTime(input);
            }
        };
        
        final TClass type;
        abstract boolean isValid(long[] datetime);
        abstract long[] decode(long input);
        
        private DateType(TClass type) {
            this.type = type;
        }
    }
 
    static enum FuncType {
        DATE(MDatetimes.DATE) {

            @Override
            long encode(long[] input) {
                return MDatetimes.encodeDate(input);
            }
        },
        TIME(MDatetimes.TIME) {

            @Override
            long encode(long[] input) {
                return MDatetimes.encodeTime(input);
            }
        };
        abstract long encode(long[] input);
        final TClass resultType;
        
        private FuncType(TClass resultType) {
            this.resultType = resultType;
        }
    }

    private final FuncType outputType;
    private final DateType dateType;
    
    public static final TOverload[] INSTANCES = {
        new MTimeAndDate(FuncType.DATE, DateType.DATETIME),
        new MTimeAndDate(FuncType.DATE, DateType.DATE),
        new MTimeAndDate(FuncType.TIME, DateType.DATETIME),
        new MTimeAndDate(FuncType.TIME, DateType.TIME)
    };
    
    MTimeAndDate(FuncType outputType, DateType dateType) {
        this.outputType = outputType;
        this.dateType = dateType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(dateType.type, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        long[] datetime = dateType.decode(inputs.get(0).getInt64());
        
        if (!dateType.isValid(datetime)) output.putNull();
        else output.putInt32((int)(outputType.encode(datetime)));
    }

    @Override
    public String overloadName() {
        return outputType.name();
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(outputType.resultType.instance());
    }
}
