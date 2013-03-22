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

import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.common.types.StringFactory;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
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
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            output.putInt32((inputs.get(0).getString()).length());
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
        protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
        {
            int charsetId = context.inputTInstanceAt(0).attribute(StringAttribute.CHARSET);
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