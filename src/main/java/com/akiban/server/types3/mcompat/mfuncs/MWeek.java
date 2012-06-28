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
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import org.joda.time.DateTimeConstants;
import org.joda.time.MutableDateTime;

public abstract class MWeek extends TOverloadBase { 
    
    private static final int DEFAULT_MODE = 0;
    private static final int WEEKOFYEAR_MODE = 3;
    private final WeekType weekType;
    private final DateType dateType;
            
    protected static enum WeekType {
        WEEK() {
            
            @Override
            int getYearWeek(int mode, long[] date, MutableDateTime datetime) {
                return modes[mode].getWeek(datetime,
                        (int) date[MDatetimes.YEAR_INDEX], (int) date[MDatetimes.MONTH_INDEX], (int) date[MDatetimes.DAY_INDEX]);
            }
        }, 
        
        WEEKOFYEAR() {

            @Override
            int getYearWeek(int mode, long[] date, MutableDateTime datetime) {
                return modes[mode].getWeek(datetime,
                        (int) date[MDatetimes.YEAR_INDEX], (int) date[MDatetimes.MONTH_INDEX], (int) date[MDatetimes.DAY_INDEX]);
            }
        },
        
        YEARWEEK() {

            @Override
            int getYearWeek(int mode, long[] date, MutableDateTime datetime) {
                return yearModes[mode].getYearWeek(datetime,
                        (int) date[MDatetimes.YEAR_INDEX], (int) date[MDatetimes.MONTH_INDEX], (int) date[MDatetimes.DAY_INDEX]);
            }
        };
        abstract int getYearWeek(int mode, long[] date, MutableDateTime datetime);
    };
    
    protected static enum DateType {
        DATETIME(MDatetimes.DATETIME) {
            
            @Override
            long[] decode(long input, TExecutionContext context) {
                return MDatetimes.decodeDatetime(input);
            }
        }, 
        
        DATE(MDatetimes.DATE) {
            
            @Override
            long[] decode(long input, TExecutionContext context) {
                return MDatetimes.decodeDate(input);
            }
        }, 
        
        TIMESTAMP(MDatetimes.TIMESTAMP) {

            @Override
            long[] decode(long input, TExecutionContext context) {
                return MDatetimes.decodeTimestamp(input, context.getCurrentTimezone() );
            }
        };
        
        abstract long[] decode(long input, TExecutionContext context);
        final TClass typeClass;
        
        private DateType(TClass dateType) {
            this.typeClass = dateType;
        }
    }
    
    public static final TOverload[] WEEK = {
        new MWeek(WeekType.WEEK, DateType.DATETIME) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                evaluateYearWeek(context, inputs.get(0).getInt64(), DEFAULT_MODE, output);
            }
        },
        new MWeek(WeekType.WEEK, DateType.DATE) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                evaluateYearWeek(context, inputs.get(0).getInt32(), DEFAULT_MODE, output);
            }
        },
        new MWeek(WeekType.WEEK, DateType.TIMESTAMP) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                evaluateYearWeek(context, inputs.get(0).getInt32(), DEFAULT_MODE, output);
            }
        },
        new MWeek(WeekType.WEEK, DateType.DATETIME) {

            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(MDatetimes.DATETIME, 0);
                builder.covers(MNumeric.INT, 1);
            }

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                int mode = inputs.get(1).getInt32();
                evaluateYearWeek(context, inputs.get(0).getInt64(), mode, output);
            }
        },
        new MWeek(WeekType.WEEK, DateType.DATE) {

            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(MDatetimes.DATE, 0);
                builder.covers(MNumeric.INT, 1);
            }

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                int mode = inputs.get(1).getInt32();
                evaluateYearWeek(context, inputs.get(0).getInt32(), mode, output);
            }
        },
        new MWeek(WeekType.WEEK, DateType.TIMESTAMP) {

            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(MDatetimes.TIMESTAMP, 0);
                builder.covers(MNumeric.INT, 1);
            }

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                int mode = inputs.get(1).getInt32();
                evaluateYearWeek(context, inputs.get(0).getInt32(), mode, output);
            }
        }
    };
    public static final TOverload[] WEEKOFYEAR = {
        new MWeek(WeekType.WEEKOFYEAR, DateType.DATETIME) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                evaluateYearWeek(context, inputs.get(0).getInt32(), WEEKOFYEAR_MODE, output);
            }
        },
        new MWeek(WeekType.WEEKOFYEAR, DateType.DATE) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                evaluateYearWeek(context, inputs.get(0).getInt32(), WEEKOFYEAR_MODE, output);
            }
        }, 
        new MWeek(WeekType.WEEKOFYEAR, DateType.TIMESTAMP) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                evaluateYearWeek(context, inputs.get(0).getInt32(), WEEKOFYEAR_MODE, output);
            }
        }
    };
    public static final TOverload[] YEARWEEK = {
        new MWeek(WeekType.YEARWEEK, DateType.DATETIME) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                evaluateYearWeek(context, inputs.get(0).getInt32(), DEFAULT_MODE, output);
            }
        },
        new MWeek(WeekType.YEARWEEK, DateType.DATE) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                evaluateYearWeek(context, inputs.get(0).getInt32(), DEFAULT_MODE, output);
            }
        },
        new MWeek(WeekType.YEARWEEK, DateType.TIMESTAMP) {

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                evaluateYearWeek(context, inputs.get(0).getInt32(), DEFAULT_MODE, output);
            }
        },
        new MWeek(WeekType.YEARWEEK, DateType.DATETIME) {

            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(MDatetimes.DATETIME, 0);
                builder.covers(MNumeric.INT, 1);
            }

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                int mode = inputs.get(1).getInt32();
                evaluateYearWeek(context, inputs.get(0).getInt32(), mode, output);
            }
        },
        new MWeek(WeekType.YEARWEEK, DateType.DATE) {

            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(MDatetimes.DATE, 0);
                builder.covers(MNumeric.INT, 1);
            }

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                int mode = inputs.get(1).getInt32();
                evaluateYearWeek(context, inputs.get(0).getInt32(), mode, output);
            }
        },
        new MWeek(WeekType.YEARWEEK, DateType.TIMESTAMP) {

            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(MDatetimes.TIMESTAMP, 0);
                builder.covers(MNumeric.INT, 1);
            }

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                int mode = inputs.get(1).getInt32();
                evaluateYearWeek(context, inputs.get(0).getInt32(), mode, output);
            }
        }
    };
       
    protected MWeek(WeekType type, DateType dateType) {
        this.weekType = type;
        this.dateType = dateType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(this.dateType.typeClass);
    }

    protected void evaluateYearWeek(TExecutionContext context, long input, int mode, PValueTarget output) {
        long[] date = dateType.decode(input, context);
        if (!isZero(date, context, output) && isModeRange(mode, context, output)) {
            MutableDateTime datetime = MDatetimes.toJodaDatetime(date, context.getCurrentTimezone());
            int week = weekType.getYearWeek(mode, date, datetime);
            output.putInt32(week);
        }
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.INT.instance());
    }

    @Override
    public String overloadName() {
        return weekType.name();
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

    private static final Modes[] modes = new Modes[]
    {
        new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DayOfWeek.SUN, 8);}}, //0
        new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DayOfWeek.SUN,8);}},  //1
        new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DayOfWeek.SUN, 0);}}, //2
        new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DayOfWeek.SUN, 1);}}, //3
        new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DayOfWeek.SAT, 8);}},//4
        new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DayOfWeek.MON, 8);}}, //5
        new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode1346(cal, yr, mo, da, DayOfWeek.SAT,4);}},//6
        new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return getMode0257(cal, yr, mo, da, DayOfWeek.MON,5);}},  //7
        new Modes() {public int getWeek(MutableDateTime cal, int yr, int mo, int da){return 0;}} // dummy always return 0-lowestval
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
    
    private static interface YearModes 
    {
        int getYearWeek(MutableDateTime cal, int yr, int mo, int da);
    }

    private static final YearModes[] yearModes = new YearModes[] {
        new YearModes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getYearMode0257(cal, yr, mo, da, DateTimeConstants.SUNDAY, 0);}}, //0
        new YearModes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getYearMode1346(cal, yr, mo, da, DateTimeConstants.SUNDAY,1);}},  //1
        new YearModes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getYearMode0257(cal, yr, mo, da, DateTimeConstants.SUNDAY, 0);}}, //2
        new YearModes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getYearMode1346(cal, yr, mo, da, DateTimeConstants.SUNDAY, 1);}}, //3
        new YearModes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getYearMode1346(cal, yr, mo, da, DateTimeConstants.SATURDAY,4);}},//4
        new YearModes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getYearMode0257(cal, yr, mo, da, DateTimeConstants.MONDAY, 5);}}, //5
        new YearModes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getYearMode1346(cal, yr, mo, da, DateTimeConstants.SATURDAY,4);}},//6
        new YearModes() {public int getYearWeek(MutableDateTime cal, int yr, int mo, int da){return getYearMode0257(cal, yr, mo, da, DateTimeConstants.MONDAY,5);}},  //7

    };

    private static int getYearMode1346(MutableDateTime cal, int yr, int mo, int da, int firstDay, int lowestVal)
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
            if (week < 0) return  yearModes[lowestVal].getYearWeek(cal, yr - 1, 12, 31);
            else return yr * 100 + week / 7 + 1;
        }
        else
        {
            if (week < 0) return yr * 100 + 1;
            else return yr * 100 + week / 7 + 2;
        }
    }

    private static int getYearMode0257(MutableDateTime cal, int yr, int mo, int da, int firstDay, int lowestVal)
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

        if (dayOfYear < firstD) return yearModes[lowestVal].getYearWeek(cal, yr - 1, 12, 31);
        else return yr * 100 + (dayOfYear - firstD) / 7 +1;
    }    

    protected static boolean isZero(long[] date, TExecutionContext context,PValueTarget output) {
        boolean isZero = date[MDatetimes.MONTH_INDEX] == 0L || date[MDatetimes.DAY_INDEX] == 0L;
        if (isZero) {
            if (context != null)
                context.warnClient(new ZeroDateTimeException());
            output.putNull();
        }
        return isZero;
    }
    
    protected static boolean isModeRange(int mode, TExecutionContext context, PValueTarget output) {
        boolean inRange = mode >= 0 && mode <8;
        if (!inRange) {
            if (context != null) {
                context.warnClient(new InvalidParameterValueException("MODE out of range [0, 7]: " + mode));
            }
            output.putNull();
        } 
        return inRange;
    }
}
