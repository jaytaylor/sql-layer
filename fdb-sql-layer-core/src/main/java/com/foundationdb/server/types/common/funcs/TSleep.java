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

package com.foundationdb.server.types.common.funcs;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.QueryCanceledException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public class TSleep extends TScalarBase
{
    private final TClass intType;
    
    public TSleep(TClass intType) {
        this.intType = intType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(intType, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
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
            if (context.getQueryContext().getSession().isCurrentQueryCanceled())
                throw new QueryCanceledException(context.getQueryContext().getSession());
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
        return TOverloadResult.fixed(intType);
    }

    @Override
    protected boolean neverConstant() {
        return true;
    }
}
