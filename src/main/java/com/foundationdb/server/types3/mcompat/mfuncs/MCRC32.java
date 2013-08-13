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

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types3.LazyList;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TScalar;
import com.foundationdb.server.types3.TOverloadResult;
import com.foundationdb.server.types3.common.types.StringAttribute;
import com.foundationdb.server.types3.common.types.StringFactory;
import com.foundationdb.server.types3.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;

import java.io.UnsupportedEncodingException;
import java.util.zip.CRC32;

public class MCRC32 extends TScalarBase
{
    public static final TScalar INSTANCE = new MCRC32();
    
    private MCRC32() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MString.VARCHAR, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        String charset = StringFactory.Charset.of(context.inputTInstanceAt(0).attribute(StringAttribute.CHARSET));
        try
        {
            CRC32 crc32 = new CRC32();
            crc32.update(inputs.get(0).getString().getBytes(charset));
            output.putInt64(crc32.getValue());
        }
        catch (UnsupportedEncodingException ex)
        {
            context.warnClient(new InvalidParameterValueException("Invalid charset: " + charset));
            output.putNull();
        }
    }

    @Override
    public String displayName()
    {
        return "crc32";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.INT_UNSIGNED);
    }
}
