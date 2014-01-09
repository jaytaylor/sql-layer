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
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.google.common.collect.ObjectArrays;

import java.io.UnsupportedEncodingException;

/**
 *
 * Implement the length (char_length and octet_length)
 */
public abstract class MLength extends TScalarBase
{
    public static final TScalar CHAR_LENGTH = new MLength("CHAR_LENGTH")
    {
        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
        {
            String str = inputs.get(0).getString();
            output.putInt32(str.codePointCount(0, str.length()));
        }

        @Override
        public String[] registeredNames() {
            return new String[] { "char_length", "charLength" };
        }
    };

    public static final TScalar OCTET_LENGTH = new MBinaryLength("OCTET_LENGTH", 1, "getOctetLength");
    public static final TScalar BIT_LENGTH = new MBinaryLength("BIT_LENGTH", 8);

    private static class MBinaryLength extends MLength
    {

        private final int multiplier;
        private final String[] aliases;

        private MBinaryLength(String name, int multiplier, String... aliases) {
            super(name);
            this.multiplier = multiplier;
            this.aliases = ObjectArrays.concat(aliases, name);
        }

        @Override
        protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
        {
            int charsetId = context.inputTypeAt(0).attribute(StringAttribute.CHARSET);
            String charset = (StringFactory.Charset.values())[charsetId].name();
            try
            {
                int length = (inputs.get(0).getString()).getBytes(charset).length;
                length *= multiplier;
                output.putInt32(length);
            }
            catch (UnsupportedEncodingException ex) // impossible to happen
            {
                context.warnClient(new InvalidParameterValueException("Unknown CHARSET: " + charset));
                output.putNull();
            }
        }

        @Override
        public String[] registeredNames() {
            return aliases;
        }
    }
    
    private final String name;

    private MLength (String name)
    {
        this.name = name;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MString.VARCHAR, 0);
    }



    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.INT, 10);
    }
}
