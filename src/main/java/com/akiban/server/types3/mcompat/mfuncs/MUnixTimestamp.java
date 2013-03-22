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

import com.akiban.server.types3.*;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

public abstract class MUnixTimestamp extends TScalarBase {
    
    public static final TScalar[] INSTANCES =
    {
        new MUnixTimestamp()
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder)
            {
                builder.covers(MDatetimes.TIMESTAMP, 0);
            }
            @Override
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
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
            protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
            {
                output.putInt32((int)MDatetimes.encodeTimetamp(context.getCurrentDate(), context));
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
