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
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public class MSecToTime extends TScalarBase
{
    public static final TScalar INSTANCE = new MSecToTime();
    
    private MSecToTime() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder) 
    {
        builder.covers(MNumeric.BIGINT, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        long time = inputs.get(0).getInt64();
        int mul;
        
        if (time < 0)
            time *= mul = -1;
        else
            mul = 1;

        long hour = time / 3600 * mul;
        long min = time / 60;
        long sec = time % 60;

        output.putInt32(MDateAndTime.encodeTime(hour, min, sec, context));
    }

    @Override
    public String displayName() {
        return "SEC_TO_TIME";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MDateAndTime.TIME);
    }
}
