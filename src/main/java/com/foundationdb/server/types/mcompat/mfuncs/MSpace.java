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
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.util.Arrays;
import java.util.List;

@SuppressWarnings("unused")
public class MSpace extends TScalarBase
{
    public static final TScalar INSTANCE = new MSpace(MString.VARCHAR, MString.LONGTEXT, MNumeric.INT);

    private final TClass stringType;
    private final TClass longTextType;
    private final TClass intType;

    MSpace(TClass stringType, TClass longTextType, TClass intType)
    {
        this.stringType = stringType;
        this.longTextType = longTextType;
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
        int count = inputs.get(0).getInt32();
        final String s;
        if (count <= 0) {
            s = "";
        } else {
            char ret[] = new char[count];
            Arrays.fill(ret, ' ');
            s = new String(ret);
        }
        output.putString(s, null);
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
                ValueSource length = inputTpv.value();
                if(length == null) {
                    return MString.LONGTEXT.instance(true);
                } else if(length.isNull()) {
                    return stringType.instance(0, true);
                } else {
                    return stringType.instance(length.getInt32(), false);
                }
            }
        });
    }
}
