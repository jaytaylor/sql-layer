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
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

/**
 * REPLACE function: simple string substitution.
 */
public class MReplace extends TScalarBase
{
    public static final TScalar INSTANCE = new MReplace();
    
    private MReplace() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.pickingCovers(MString.VARCHAR, 0);
        builder.covers(MString.VARCHAR, 1, 2);
    }

    @Override
    public String displayName()
    {
        return "REPLACE";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.picking();        
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        // MySQL is case-sensitive, independent of collation.
        String str = inputs.get(0).getString();
        String target = inputs.get(1).getString();
        String replacement = inputs.get(2).getString();
        output.putString(str.replace(target, replacement), null);
    }
}
