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

import java.util.List;

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.util.Strings;

public class Repeat extends TScalarBase {

    private final TClass stringType;
    private final TClass intType;
    
    public Repeat (TClass stringType, TClass intType) {
        this.stringType = stringType;
        this.intType = intType;
    }
    
    @Override
    public String displayName() {
        return "REPEAT";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                ValueSource string = inputs.get(0).value();
                int strLen;
                if (string == null || string.isNull()) {
                    strLen = 0;
                } else {
                    strLen = string.getString().length();
                }
                ValueSource length = inputs.get(1).value();
                int count;
                if (length == null || length.isNull() || (count = length.getInt32()) <= 0) {
                    return stringType.instance(0, anyContaminatingNulls(inputs));
                } else {
                    return stringType.instance(count * strLen, anyContaminatingNulls(inputs));
                }
            }
        });
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(stringType, 0).covers(intType, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context,
            LazyList<? extends ValueSource> inputs, ValueTarget output) {
        String st = inputs.get(0).getString();
        if (st.isEmpty()) {
            output.putString("", null);
            return;
        }
        int count = inputs.get(1).getInt32();

        if (count <= 0) {
            output.putString("", null);
            return;
        }
        output.putString(Strings.repeatString(st, count), null);
    }
}