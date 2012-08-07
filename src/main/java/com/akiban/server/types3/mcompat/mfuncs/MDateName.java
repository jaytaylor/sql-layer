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
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import java.text.DateFormatSymbols;
import java.util.Locale;
import org.joda.time.MutableDateTime;

public class MDateName extends TOverloadBase {
    
    private static final String MONTHS[] = new DateFormatSymbols(new Locale(System.getProperty("user.language"))).getMonths();
    private static final String DAYS[] = new DateFormatSymbols(new Locale(System.getProperty("user.language"))).getWeekdays();

    private final FuncType funcType;
    
    static enum FuncType {
        MONTHNAME() {
            @Override
            void evaluate(TExecutionContext context, long[] dateArr, PValueTarget output) {
                // TODO: update with new method that gets locale
                String month = MONTHS[(int)dateArr[MDatetimes.MONTH_INDEX]];
                output.putString(month, null);
            }
        }, 
        DAYNAME() {
            @Override
            void evaluate(TExecutionContext context, long[] dateArr, PValueTarget output) {
                MutableDateTime datetime = MDatetimes.toJodaDatetime(dateArr, context.getCurrentTimezone());
                String day = DAYS[datetime.getDayOfWeek()%7];
                output.putString(day, null);
            }
        };
        abstract void evaluate(TExecutionContext context, long[] dateArr, PValueTarget output);
    }
    
    public static final TOverload[] INSTANCES = {
        new MDateName(FuncType.MONTHNAME),
        new MDateName(FuncType.DAYNAME)
    };
    
    private MDateName(FuncType funcType) {
        this.funcType = funcType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MDatetimes.DATETIME, 0);
    }
    
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        funcType.evaluate(context, MDatetimes.decodeDatetime(inputs.get(0).getInt64()), output);
    }
    
    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MString.VARCHAR.instance());
    }

    @Override
    public String displayName() {
        return funcType.name();
    }
}
