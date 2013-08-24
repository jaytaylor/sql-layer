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

package com.foundationdb.server.types3.mcompat.mfuncs;

import com.foundationdb.server.types3.LazyList;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TScalar;
import com.foundationdb.server.types3.TOverloadResult;
import com.foundationdb.server.types3.mcompat.mtypes.MDatetimes;
import com.foundationdb.server.types3.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class MFromDays extends TScalarBase
{
    public static final TScalar INSTANCE = new MFromDays();
    
    private MFromDays(){};

    private static final long BEGINNING = new DateTime(0, 1, 1,
                                                       0, 0, 0, DateTimeZone.UTC).getMillis();
    private  static final long FACTOR = 3600L * 1000 * 24;

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MNumeric.BIGINT, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        long val = inputs.get(0).getInt64();
        output.putInt32( val < 366
                ? 0
                : MDatetimes.encodeDate(val * FACTOR + BEGINNING, "UTC"));
    }

    @Override
    public String displayName()
    {
        return "FROM_DAYS";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MDatetimes.DATE);
    }
}
