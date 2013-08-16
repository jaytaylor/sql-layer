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

import com.foundationdb.server.types3.LazyList;
import com.foundationdb.server.types3.TCustomOverloadResult;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.TScalar;
import com.foundationdb.server.types3.TOverloadResult;
import com.foundationdb.server.types3.TPreptimeContext;
import com.foundationdb.server.types3.TPreptimeValue;
import com.foundationdb.server.types3.common.types.StringAttribute;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;
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
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        String delim = inputs.get(0).getString();
        StringBuilder ret = new StringBuilder();

        for (int n = 1; n < inputs.size(); ++n)
        {
            PValueSource source = inputs.get(n);
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
                int dLen = inputs.get(0).instance().attribute(StringAttribute.MAX_LENGTH);
                int len = 0;
                
                for (int n = 1; n < inputs.size(); ++n)
                    len += inputs.get(n).instance().attribute(StringAttribute.MAX_LENGTH) + dLen;
                
                // delele the laste delimeter
                len -= dLen;
                
                return MString.VARCHAR.instance(len, anyContaminatingNulls(inputs));
            }
        });
    }
}
