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

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.mcompat.mtypes.MDateAndTime;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public class MMaketime extends TScalarBase {

    public static final TScalar INSTANCE = new MMaketime() {};
    
    private MMaketime() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MNumeric.INT, 0, 1, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        // Time input format HHMMSS
        int hours = inputs.get(0).getInt32();
        int minutes = inputs.get(1).getInt32();
        int seconds = inputs.get(2).getInt32();
        
        // Check for invalid input
        if (minutes < 0 || minutes >= 60 || seconds < 0 || seconds >= 60) {
            output.putNull();
            return;
        }
        
        int mul;
        if (hours < 0)
            hours *= mul = -1;
        else
            mul = 1;

        output.putInt32(MDateAndTime.encodeTime(hours, minutes, seconds, context));
    }

    @Override
    public String displayName() {
        return "MAKETIME";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MDateAndTime.TIME);
    }
}
