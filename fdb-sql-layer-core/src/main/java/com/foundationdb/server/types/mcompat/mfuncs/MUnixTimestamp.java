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

public abstract class MUnixTimestamp extends TScalarBase {
    
    public static final TScalar[] INSTANCES =
    {
        new MUnixTimestamp()
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(MDateAndTime.TIMESTAMP, 0);
            }
            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
            {
                output.putInt32(inputs.get(0).getInt32());
            }            
        },
        new MUnixTimestamp() {

            @Override
            protected void buildInputSets(TInputSetBuilder builder) 
            {
                // Does nothing (takes 0 input)
            }

            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
            {
                output.putInt32((int)MDateAndTime.encodeTimestamp(context.getCurrentDate(), context));
            }
        }
    };

    @Override
    public String displayName() 
    {
        return "UNIX_TIMESTAMP";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(MNumeric.INT);
    }
}
