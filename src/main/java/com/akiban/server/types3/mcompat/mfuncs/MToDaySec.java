/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.error.InvalidDateFormatException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public abstract class MToDaySec extends TScalarBase
{
    public static final TScalar TO_DAYS = new MToDaySec(MDatetimes.DATETIME, "TO_DAYS")
    {
        @Override
        protected int computeDaySec(long[] ymd)
        {
            long now = MDatetimes.toJodaDatetime(ymd, "UTC").getMillis();
            return (int)((now - START) / MILLIS_PER_DAY);
        }
    };

    public static final TScalar TO_SECS = new MToDaySec(MDatetimes.DATETIME, "TO_SECONDS")
    {
        @Override
        protected int computeDaySec(long[] ymd)
        {
            long now = MDatetimes.toJodaDatetime(ymd, "UTC").getMillis();
            return (int)((now - START) / MILLIS_PER_SEC);
        }    
    };
    
    public static final TScalar TIME_TO_SEC = new MToDaySec(MDatetimes.TIME, "TIME_TO_SEC")
    {
        @Override
        public TOverloadResult resultType()
        {
            return TOverloadResult.fixed(MNumeric.INT, 10);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            long hms[] = MDatetimes.decodeTime(inputs.get(0).getInt32());
            
            if (!MDatetimes.isValidHrMinSec(hms, false))
            {
                context.warnClient(new InvalidDateFormatException("time",
                                                                  MDatetimes.timeToString((int)inputs.get(0).getInt32())));
                output.putNull();
            }
            else
            {
                output.putInt32((int)(hms[MDatetimes.HOUR_INDEX] * SEC_PER_HOUR
                                       + hms[MDatetimes.MIN_INDEX] * SEC_PER_MIN
                                       + hms[MDatetimes.SEC_INDEX]));
            }
        }

        @Override
        protected int computeDaySec(long[] ymd)
        {
            throw new AssertionError("Should not be used");
        }   
    };
    
    private static final int SEC_PER_HOUR = 3600;
    private static final int SEC_PER_MIN = 60;
    private static final int MILLIS_PER_SEC = 1000;
    private static final int MILLIS_PER_DAY = 24 * 60 * 60 * 1000;
    private static final long START = new DateTime(0, 1, 1, 
                                                   0, 0, 0, 0,
                                                   DateTimeZone.UTC).getMillis();
    

    private final TClass inputType;
    private final String name;

    abstract protected int computeDaySec(long ymd[]);

    private MToDaySec(TClass inputType, String name)
    {
        this.inputType = inputType;
        this.name = name;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(inputType, 0);
    }
    
    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        long ymd[] = MDatetimes.decodeDatetime(inputs.get(0).getInt64());
        if (!MDatetimes.isValidDatetime(ymd))
        {
            context.warnClient(new InvalidDateFormatException("DATETIME",
                                                              MDatetimes.datetimeToString(inputs.get(0).getInt64())));

            output.putNull();
        }
        else
            output.putInt32(computeDaySec(ymd));
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.INT, 6);
    }
}
