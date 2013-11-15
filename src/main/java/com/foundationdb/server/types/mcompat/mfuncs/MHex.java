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
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.mcompat.mtypes.MBinary;
import com.foundationdb.server.types.mcompat.mtypes.MBinary.Attrs;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.util.Strings;
import com.google.common.primitives.Ints;

import java.nio.charset.Charset;
import java.util.List;

@SuppressWarnings("unused")
public abstract class MHex extends TScalarBase
{
    private static final String HEX_NAME = "HEX";


    private static Charset getCharset(TInstance inst) {
        int id = inst.attribute(StringAttribute.CHARSET);
        String name = (StringFactory.Charset.values())[id].name();
        return Charset.forName(name);
    }


    public static final TScalar HEX_STRING = new TScalarBase()
    {
        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            builder.covers(MString.VARCHAR, 0);
        }

        @Override
        protected void doEvaluate(TExecutionContext context,
                                  LazyList<? extends ValueSource> inputs,
                                  ValueTarget output) {
            Charset charset = getCharset(context.inputTInstanceAt(0));
            String s = inputs.get(0).getString();
            byte[] bytes = s.getBytes(charset);
            output.putString(Strings.hex(bytes), null);
        }

        @Override
        public String displayName() {
            return HEX_NAME;
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.custom(new TCustomOverloadResult() {
                @Override
                public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                    TInstance inst = inputs.get(0).instance();
                    int maxLen = inst.attribute(StringAttribute.MAX_LENGTH);
                    Charset charset = getCharset(inst);
                    long maxBytes = (long)Math.ceil(maxLen * charset.newEncoder().maxBytesPerChar());
                    long maxHexLength = maxBytes * 2;
                    return MString.VARCHAR.instance(Ints.saturatedCast(maxHexLength), anyContaminatingNulls(inputs));
                }
            });
        }

    };

    public static final TScalar HEX_BIGINT = new TScalarBase()
    {
        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            builder.covers(MNumeric.BIGINT, 0);
        }

        @Override
        protected void doEvaluate(TExecutionContext context,
                                  LazyList<? extends ValueSource> inputs,
                                  ValueTarget output) {
            long value = inputs.get(0).getInt64();
            output.putString(Long.toHexString(value).toUpperCase(), null);
        }

        @Override
        public String displayName() {
            return HEX_NAME;
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.custom(new TCustomOverloadResult() {
                @Override
                public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                    // 16 = BIGINT size * 2
                    return MString.VARCHAR.instance(16, anyContaminatingNulls(inputs));
                }
            });
        }
    };

    public static final TScalar HEX_BINARY = new TScalarBase()
    {
        @Override
        protected void buildInputSets(TInputSetBuilder builder) {
            builder.covers(MBinary.VARBINARY, 0);
        }

        @Override
        protected void doEvaluate(TExecutionContext context,
                                  LazyList<? extends ValueSource> inputs,
                                  ValueTarget output) {
            byte[] bytes = inputs.get(0).getBytes();
            output.putString(Strings.hex(bytes), null);
        }

        @Override
        public String displayName() {
            return HEX_NAME;
        }

        @Override
        public TOverloadResult resultType() {
            return TOverloadResult.custom(new TCustomOverloadResult() {
                @Override
                public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                    int length = inputs.get(0).instance().attribute(Attrs.LENGTH);
                    return MString.VARCHAR.instance(Ints.saturatedCast(length * 2), anyContaminatingNulls(inputs));
                }
            });
        }
    };
}
