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
import com.akiban.server.types3.texpressions.TOverloadBase;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

public class MToDatetime extends TOverloadBase {
    
    private static final long SECONDS_FACTOR = 100L;
    private static final long DAY_FACTOR = 3600L * 1000 * 24;
    private static final long BEGINNING = encodeBeginning();

    private static long encodeBeginning() {
        MutableDateTime dt = new MutableDateTime(1,1,1,0,0,0,0);
        dt.setZone(DateTimeZone.UTC);
        return dt.getMillis();
    }

    private final DateType dateType;
    private final FuncType funcType;
    
    static enum DateType {
        DATETIME(MDatetimes.DATETIME) {
            @Override
            long[] decode(long input) {
                return MDatetimes.decodeDatetime(input);
            }
        },
        DATE(MDatetimes.DATE) {
            @Override
            long[] decode(long input) {
                return MDatetimes.decodeDate(input);
            }
        }, 
        TIME(MDatetimes.TIME) {
            @Override
            long[] decode(long input) {
                return MDatetimes.decodeTime(input);
            }
        };
        abstract long[] decode(long input);
        final TClass type;
        
        private DateType(TClass type) {
            this.type = type;
        }
    }
    
    static enum FuncType {
        TO_DAYS() {
            @Override
            void evaluate(TExecutionContext context, long[] dateArr, PValueTarget output) {
                MutableDateTime datetime = MDatetimes.toJodaDatetime(dateArr, context.getCurrentTimezone());
                long time = (datetime.getMillis() - BEGINNING) / DAY_FACTOR;
                output.putInt32((int) time);
                 
            }
        }, 
        TO_SECONDS() {
            @Override
            void evaluate(TExecutionContext context, long[] dateArr, PValueTarget output) {
                MutableDateTime datetime = MDatetimes.toJodaDatetime(dateArr, context.getCurrentTimezone());
                long seconds = (datetime.getMillis() - BEGINNING) / SECONDS_FACTOR;
                output.putInt32((int) seconds);
            }
        }, 
        TIME_TO_SEC() {
            @Override
            void evaluate(TExecutionContext context, long[] dateArr, PValueTarget output) {
                int hoursToSec = (int) dateArr[MDatetimes.HOUR_INDEX] * 3600;
                int minutesToSec = (int) dateArr[MDatetimes.MIN_INDEX] * 60;
                int seconds = (int) dateArr[MDatetimes.SEC_INDEX] + hoursToSec + minutesToSec;
                int result = MDatetimes.encodeTime(seconds*1000, context.getCurrentTimezone());
                output.putInt32(result);
            }
        };
        abstract void evaluate(TExecutionContext context, long[] dateArr, PValueTarget output);
    }
    
    public static final TOverload[] INSTANCES = {
        new MToDatetime(FuncType.TO_DAYS, DateType.DATETIME),
        new MToDatetime(FuncType.TO_DAYS, DateType.DATE),
        new MToDatetime(FuncType.TIME_TO_SEC, DateType.DATETIME),
        new MToDatetime(FuncType.TIME_TO_SEC, DateType.TIME),
        new MToDatetime(FuncType.TO_SECONDS, DateType.DATETIME),
        new MToDatetime(FuncType.TO_SECONDS, DateType.TIME)
    };
    
    private MToDatetime(FuncType funcType, DateType dateType) {
        this.funcType = funcType;
        this.dateType = dateType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(dateType.type, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        funcType.evaluate(context, dateType.decode(inputs.get(0).getInt64()), output);
    }
    
    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.INT.instance());
    }

    @Override
    public String displayName() {
        return funcType.name();
    }
}
