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
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import java.util.List;

public class MConcatWS extends TScalarBase
{
    public static final TScalar INSTANCE = new MConcatWS();
    
    private MConcatWS() {}
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        // the function should have at least 2 args
        builder.vararg(MString.VARCHAR, 0, 1);
    }
    
    @Override
    protected boolean nullContaminates(int inputIndex)
    {
        return inputIndex == 0;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        String delim = inputs.get(0).getString();
        StringBuilder ret = new StringBuilder();

        for (int n = 1; n < inputs.size(); ++n)
        {
            ValueSource source = inputs.get(n);
            if (!source.isNull())
                ret.append(source.getString()).append(delim);
        }
        if (ret.length()!= 0)
            ret.delete(ret.length() - delim.length(),
                       ret.length());

        output.putString(ret.toString(), null);
    }

    @Override
    public String displayName()
    {
        return "CONCAT_WS";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                int dLen = inputs.get(0).type().attribute(StringAttribute.MAX_LENGTH);
                int len = 0;
                
                for (int n = 1; n < inputs.size(); ++n)
                    len += inputs.get(n).type().attribute(StringAttribute.MAX_LENGTH) + dLen;
                
                // delele the laste delimeter
                len -= dLen;
                
                return MString.VARCHAR.instance(len, anyContaminatingNulls(inputs));
            }
        });
    }
}
