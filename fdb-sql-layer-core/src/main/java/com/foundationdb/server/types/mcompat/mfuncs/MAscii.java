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

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.io.UnsupportedEncodingException;

/**
 * ASCII function: return single encoded byte.
 */
public class MAscii extends TScalarBase
{
    public static final TScalar INSTANCE = new MAscii();
    
    private MAscii() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MString.VARCHAR, 0);
    }

    @Override
    public String displayName()
    {
        return "ascii";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.INT, 3);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        ValueSource sval = inputs.get(0);
        String str = sval.getString();
        int code = 0;
        if (str.length() == 0) {
            output.putInt32(0);
        }
        else {
            String charset = StringFactory.Charset.of(sval.getType().attribute(StringAttribute.CHARSET));
            try {
                byte[] bytes = str.substring(0, 1).getBytes(charset);
                output.putInt32(bytes[0] & 0xFF);
            }
            catch (UnsupportedEncodingException ex) {
                context.warnClient(new InvalidParameterValueException("Invalid charset: " + charset));
                output.putNull();
            }
        }
    }
}
