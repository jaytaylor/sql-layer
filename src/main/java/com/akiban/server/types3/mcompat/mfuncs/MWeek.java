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

import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.error.ZeroDateTimeException;
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

public abstract class MWeek extends TOverloadBase {

    private static final int WEEK_INDEX = 0; 
    
    public static final MWeek[] INSTANCES = {
        new MWeek() {

        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            builder.covers(MDatetimes.DATE, 0);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int input = inputs.get(0).getInt32();
            
            long[] date = DateExtractor.extract(input);
            if (isZero(date, context, output)) return;
        
            output.putInt32(getWeek(0, context, date));
        }
    },
        new MWeek() {

        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            builder.covers(MDatetimes.DATE, 0);
            builder.covers(MNumeric.INT, 1);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
            int input = inputs.get(0).getInt32();
            
            long[] date = DateExtractor.extract(input);
            if (isZero(date, context, output)) return;
           
            int mode = inputs.get(1).getInt32();
            if (mode < 0 || mode > 7) {
                if (context != null)
                    context.warnClient(new InvalidParameterValueException("MODE out of range [0, 7]: " + mode));
                output.putNull(); 
            }
            else {
                output.putInt32(getWeek(mode, context, date));
            }        
        }
    }};

    @Override
    public String overloadName() {
        return "WEEK";
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
    
    private static final class DayOfWeek
        {
            public static final int MON = 1;
            public static final int TUE = 2;
            public static final int WED = 3;
            public static final int THU = 4;
            public static final int FRI = 5;
            public static final int SAT = 6;
            public static final int SUN = 7;
        }
        private static interface Modes
        {
            int getWeek(MutableDateTime cal, int yr, int mo, int da);
        }

        private static final MWeek.Modes[] modes = new MWeek.Modes[]
        {
          new MWeek.Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, MWeek.DayOfWeek.SUN, 8);}}, //0
          new MWeek.Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, MWeek.DayOfWeek.SUN,8);}},  //1
          new MWeek.Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, MWeek.DayOfWeek.SUN, 0);}}, //2
          new MWeek.Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, MWeek.DayOfWeek.SUN, 1);}}, //3
          new MWeek.Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, MWeek.DayOfWeek.SAT, 8);}},//4
          new MWeek.Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, MWeek.DayOfWeek.MON, 8);}}, //5
          new MWeek.Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, MWeek.DayOfWeek.SAT,4);}},//6
          new MWeek.Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, MWeek.DayOfWeek.MON,5);}},  //7
          new MWeek.Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return 0;}} // dummy always return 0-lowestval
        };

        private static int getMode1346(MutableDateTime cal, int yr, int mo, int da, int firstDay, int lowestVal)
        {
            cal.setYear(yr);
            cal.setMonthOfYear(1);
            cal.setDayOfMonth(1);

            int firstD = 1;

            while (cal.getDayOfWeek() != firstDay) 
                cal.setDayOfMonth(++firstD);

            cal.setYear(yr);
            cal.setMonthOfYear(mo);
            cal.setDayOfMonth(da);

            int week = cal.getDayOfYear() - (firstD +1 ); // Sun/Mon
            if (firstD < 4)
            {
                if (week < 0) return modes[lowestVal].getWeek(cal, yr-1, 12, 31);
                else return week / 7 + 1;
            }
            else
            {
                if (week < 0) return 1;
                else return week / 7 + 2;
            }
        }

        private static int getMode0257(MutableDateTime cal, int yr, int mo, int da, int firstDay, int lowestVal)
        {
            cal.setYear(yr);
            cal.setMonthOfYear(1);
            cal.setDayOfMonth(1);
            int firstD = 1;

            while (cal.getDayOfWeek() != firstDay)
                cal.setDayOfMonth(++firstD);

            cal.setYear(yr); 
            cal.setMonthOfYear(mo); 
            cal.setDayOfMonth(da); 

            int dayOfYear = cal.getDayOfYear(); 

            if (dayOfYear < firstD) return modes[lowestVal].getWeek(cal, yr-1, 12, 31);
            else return (dayOfYear - firstD) / 7 +1;
        }   

        private static boolean isZero(long[] date, TExecutionContext context,PValueTarget output) {
            boolean isZero = date[1] * date[2] == 0L;
            if (isZero) {
                if (context != null)
                    context.warnClient(new ZeroDateTimeException());
                output.putNull();
            }
            return isZero;
        }
        
        private static int getWeek(int mode, TExecutionContext context, long[] date) {
            MutableDateTime datetime = (MutableDateTime) context.exectimeObjectAt(WEEK_INDEX);
            if (datetime == null) context.putExectimeObject(WEEK_INDEX, datetime = new MutableDateTime());
            return modes[mode].getWeek(datetime, (int)date[0], (int)date[1], (int)date[2]);
        }
}
