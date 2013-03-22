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

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TCustomOverloadResult;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

import java.util.Arrays;
import java.util.List;

public class MSpace extends TScalarBase
{
    public static final TScalar INSTANCE = new MSpace(MString.VARCHAR, MNumeric.INT);

    private final TClass stringType;
    private final TClass intType;
    
    MSpace(TClass stringType, TClass intType)
    {
        this.stringType = stringType;
        this.intType = intType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(stringType, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        int count = inputs.get(0).getInt32();
        if (count <= 0)
            output.putString("", null);
        else
        {
            char ret[] = new char[count];
            Arrays.fill(ret, ' ');

            output.putString(new String(ret), null);
        }
    }

    @Override
    public String displayName()
    {
        return "SPACE";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                TPreptimeValue inputTpv = inputs.get(0);
                PValueSource length = inputTpv.value();
                int count;
                
                // TODO:
                // if length operand is not available,
                // the default return type is LONGTEXT
                if (length == null)
                    throw new UnsupportedOperationException("LONGTEXT type is not supported yet");
                else if (length.isNull() || (count = length.getInt32()) == 0)
                    return stringType.instance(0, inputTpv.isNullable());
                else if (count < 0)
                    throw new UnsupportedOperationException("LONGTEXT type is not supported yet");
                else
                    return stringType.instance(count, inputTpv.isNullable());
            }
        });
    }
}
