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

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.util.Strings;
import com.google.common.primitives.Ints;

import java.nio.charset.Charset;
import java.util.List;

public class Hex
{
    private Hex() {
    }

    private static final String HEX_NAME = "HEX";

    private static Charset getCharset(TInstance type) {
        return Charset.forName(StringAttribute.charsetName(type));
    }

    public static final TScalar[] create(final TString stringType, final TClass longType, final TBinary binaryType) {

        TScalar hex_string = new TScalarBase()
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(stringType, 0);
            }

            @Override
            protected void doEvaluate(TExecutionContext context,
                                      LazyList<? extends ValueSource> inputs,
                                      ValueTarget output) {
                ValueSource input = inputs.get(0);
                Charset charset = getCharset(input.getType());
                String s = input.getString();
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
                        TInstance type = inputs.get(0).type();
                        int maxLen = type.attribute(StringAttribute.MAX_LENGTH);
                        Charset charset = getCharset(type);
                        long maxBytes = (long)Math.ceil(maxLen * charset.newEncoder().maxBytesPerChar());
                        long maxHexLength = maxBytes * 2;
                        return stringType.instance(Ints.saturatedCast(maxHexLength), anyContaminatingNulls(inputs));
                    }
                });
            }

        };

        TScalar hex_bigint = new TScalarBase()
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(longType, 0);
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
                        return stringType.instance(16, anyContaminatingNulls(inputs));
                    }
                });
            }
        };

        TScalar hex_binary = new TScalarBase()
        {
            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(binaryType, 0);
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
                        int length = inputs.get(0).type().attribute(TBinary.Attrs.LENGTH);
                        return stringType.instance(Ints.saturatedCast(length * 2), anyContaminatingNulls(inputs));
                    }
                });
            }
        };

        return new TScalar[] { hex_string, hex_bigint, hex_binary };
    }

}
