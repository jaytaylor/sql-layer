/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.util.Arrays;

/**
 * 
 * implement TIMESTAMP(<expr>), DATE(<expr>), TIME(<expr>), ... functions
 */
public abstract class MExtract extends TScalarBase
{
    public static TScalar[] create()
    {
        return new TScalar[]
        {
            new MExtract(MDatetimes.DATE, "DATE")
            {

                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
                {
                    int date = inputs.get(0).getInt32();
                    long ymd[] = MDatetimes.decodeDate(date);

                    if (!MDatetimes.isValidDateTime_Zeros(ymd))
                    {
                        context.reportBadValue("Invalid DATE value " + date);
                        output.putNull();
                    }
                    else
                        output.putInt32(date);
                }
            },
            new MExtract(MDatetimes.TIME, MDatetimes.DATE, "DATE", true)
            {

                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
                {
                    output.putInt32(0);
                }
            },
            new MExtract(MDatetimes.DATETIME, "TIMESTAMP")
            {
                @Override
                public String[] registeredNames()
                {
                    return new String[] {"timestamp", "datetime"};
                }

                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
                {
                    long datetime = inputs.get(0).getInt64();
                    long ymd[] = MDatetimes.decodeDateTime(datetime);

                    if (!MDatetimes.isValidDateTime_Zeros(ymd))
                    {
                        context.reportBadValue("Invalid DATETIME value " + datetime);
                        output.putNull();
                    }
                    else
                        output.putInt64(datetime);
                }
            },
            new MExtract(MDatetimes.TIME, MDatetimes.DATETIME, "DATETIME", true)
            {
                @Override
                public String[] registeredNames()
                {
                    return new String[] {"timestamp", "datetime"};
                }

                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
                {
                    int time = inputs.get(0).getInt32();
                    long ymdHMS[] = MDatetimes.decodeTime(time);
                    Arrays.fill(ymdHMS, 0, 3, 0); // zero ymd
                    output.putInt64(MDatetimes.encodeDateTime(ymdHMS));
                }
            },
            new MExtract(MDatetimes.TIME, "TIME")
            {
                @Override
                protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
                {
                    int time = inputs.get(0).getInt32();
                    long hms[] = MDatetimes.decodeTime(time);

                    if (!MDatetimes.isValidHrMinSec(hms, false, false))
                    {
                        context.reportBadValue("Invalid TIME value: " + time);
                        output.putNull();
                    }
                    else
                        output.putInt32(time);
                }
            }
        };
    }
    
    private final TClass input, output;
    private final String name;
    private final boolean exact;
    
    private MExtract (TClass type, String name)
    {
        this(type, type, name, false);
    }

    private MExtract (TClass input, TClass output, String name, boolean exact)
    {
        this.input = input;
        this.output = output;
        this.name = name;
        this.exact = exact;
    }

    @Override
    public int[] getPriorities() {
        // The exact one has priority but only works for its particular case.
        return new int[] { exact ? 1 : 2 };
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.setExact(exact).covers(input, 0);
    }

    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(output);
    }
}
