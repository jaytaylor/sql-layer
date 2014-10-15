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

import com.foundationdb.server.error.InvalidParameterValueException;
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
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.util.Strings;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.List;

public class Base64
{
    private Base64() {
    }

    public static final TScalar[] create(final TString varchar, final TBinary varbinary) {

        TScalar binary_to_base64 = new TScalarBase()
        {
            @Override
            public String displayName() {
                return "TO_BASE64";
            }

            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(varbinary, 0);
            }

            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.custom(new TCustomOverloadResult() {
                    @Override
                    public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                        TInstance inputType = inputs.get(0).type();
                        int binaryLength = inputType.attribute(TBinary.Attrs.LENGTH);
                        int base64Length = (binaryLength * 4 + 2) / 3; // round up for ='s
                        return varchar.instance(base64Length, inputType.nullability());
                    }        
                });
            }

            @Override
            protected void doEvaluate(TExecutionContext context,
                                      LazyList<? extends ValueSource> inputs,
                                      ValueTarget output) {
                byte[] binary = inputs.get(0).getBytes();
                output.putString(Strings.toBase64(binary), null);
            }
        };

        TScalar string_to_base64 = new TScalarBase()
        {
            @Override
            public String displayName() {
                return "TO_BASE64";
            }

            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(varchar, 0);
            }

            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.custom(new TCustomOverloadResult() {
                    @Override
                    public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                        TInstance inputType = inputs.get(0).type();
                        int stringLength = inputType.attribute(StringAttribute.MAX_LENGTH);
                        int encodedLength = (int)Math.ceil(stringLength * Charset.forName(StringFactory.Charset.of(inputType.attribute(StringAttribute.CHARSET))).newEncoder().maxBytesPerChar());
                        int base64Length = (encodedLength * 4 + 2) / 3; // round up for ='s
                        return varchar.instance(base64Length, inputType.nullability());
                    }        
                });
            }

            @Override
            protected void doEvaluate(TExecutionContext context,
                                      LazyList<? extends ValueSource> inputs,
                                      ValueTarget output) {
                String charset = StringFactory.Charset.of(context.inputTypeAt(0).attribute(StringAttribute.CHARSET));
                String string = inputs.get(0).getString();
                try {
                    byte[] binary = string.getBytes(charset);
                    output.putString(Strings.toBase64(binary), null);
                }
                catch (UnsupportedEncodingException ex)
                {
                    context.warnClient(new InvalidParameterValueException("Unknown CHARSET: " + charset));
                    output.putNull();
                }
            }
        };

        TScalar from_base64 = new TScalarBase()
        {
            @Override
            public String displayName() {
                return "FROM_BASE64";
            }

            @Override
            protected void buildInputSets(TInputSetBuilder builder) {
                builder.covers(varchar, 0);
            }

            @Override
            public TOverloadResult resultType() {
                return TOverloadResult.custom(new TCustomOverloadResult() {
                    @Override
                    public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                        TInstance inputType = inputs.get(0).type();
                        int stringLength = inputType.attribute(StringAttribute.MAX_LENGTH);
                        int binaryLength = stringLength / 4 * 3;
                        return varbinary.instance(binaryLength, inputType.nullability());
                    }        
                });
            }

            @Override
            protected void doEvaluate(TExecutionContext context,
                                      LazyList<? extends ValueSource> inputs,
                                      ValueTarget output) {
                String base64 = inputs.get(0).getString();
                output.putBytes(Strings.fromBase64(base64));
            }
        };

        return new TScalar[] { binary_to_base64, string_to_base64, from_base64 };
    }

}
