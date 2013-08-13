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

package com.foundationdb.server.types3.mcompat.mfuncs;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.types3.LazyList;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TOverloadResult;
import com.foundationdb.server.types3.TScalar;
import com.foundationdb.server.types3.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;

public class MSleep extends TScalarBase
{

    public static final TScalar SLEEP = new MSleep();
    
    private MSleep() {}
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MNumeric.INT, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        try
        {
            int value = inputs.get(0).getInt32();
            Thread.sleep(value);
            output.putInt32(value);
        }
        catch (InterruptedException ex)
        {
            output.putNull();
            throw new AkibanInternalException("InteruptedException " + ex);
        }
    }

    @Override
    public String displayName()
    {
        return "sleep";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.INT);
    }
    
}
