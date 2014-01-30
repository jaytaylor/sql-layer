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

import com.foundationdb.server.error.InvalidDateFormatException;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import org.joda.time.DateTimeZone;
import org.joda.time.MutableDateTime;

@SuppressWarnings("unused")
public class MConvertTZ extends TScalarBase
{
    public static final TScalar INSTANCE = new MConvertTZ();

    private MConvertTZ()
    {}
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MDatetimes.DATETIME, 0).covers(MString.VARCHAR, 1, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        long original = inputs.get(0).getInt64();
        long[] ymd = MDatetimes.decodeDateTime(original);
        if(MDatetimes.isZeroDayMonth(ymd)) {
            output.putNull();
        } else {
            try {
                DateTimeZone fromTz = MDatetimes.parseTimeZone(inputs.get(1).getString());
                DateTimeZone toTz = MDatetimes.parseTimeZone(inputs.get(2).getString());
                MutableDateTime date = MDatetimes.toJodaDateTime(ymd, fromTz);
                // If the value falls out of the supported range of the TIMESTAMP
                // when converted from fromTz to UTC, no conversion occurs.
                date.setZone(DateTimeZone.UTC);
                final long converted;
                if(MDatetimes.isValidTimestamp(date)) {
                    date.setZone(toTz);
                    converted = MDatetimes.encodeDateTime(date);
                } else {
                    converted = original;
                }
                output.putInt64(converted);
            } catch(InvalidDateFormatException e) {
                context.warnClient(e);
                output.putNull();
            }
        }
    }

    @Override
    public String displayName()
    {
        return "CONVERT_TZ";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MDatetimes.DATETIME);
    }
}
