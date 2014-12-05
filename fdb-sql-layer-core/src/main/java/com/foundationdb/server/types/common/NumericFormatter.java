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

package com.foundationdb.server.types.common;

import com.foundationdb.server.rowdata.ConversionHelperBigDecimal;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.TClassFormatter;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.common.types.DecimalAttribute;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.AkibanAppender;
import com.foundationdb.util.Strings;
import com.google.common.primitives.UnsignedLongs;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Formatter;
import java.util.Locale;

public class NumericFormatter {

    public static enum FORMAT implements TClassFormatter {
        FLOAT {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(Float.toString(source.getFloat()));
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                new Formatter(out.getAppendable(), Locale.US).format("%e", source.getFloat());
            }
        },
        DOUBLE {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(Double.toString(source.getDouble()));
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                new Formatter(out.getAppendable(), Locale.US).format("%e", source.getDouble());
            }
        },
        INT_8 {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(Byte.toString(source.getInt8()));
            }
        },
        INT_16 {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(Short.toString(source.getInt16()));
            }
        },
        INT_32 {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(Integer.toString(source.getInt32()));
            }
        },
        INT_64 {
            
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(Long.toString(source.getInt64()));
            }
        },
        UINT_64 {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(UnsignedLongs.toString(source.getInt64()));
            }
        },
        BYTES {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                String charsetName = StringFactory.DEFAULT_CHARSET.name();
                Charset charset = Charset.forName(charsetName);
                String str = new String(source.getBytes(), charset);
                out.append(str);
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                byte[] value = source.getBytes();
                out.append("X'");
                out.append(Strings.hex(value));
                out.append("'");
            }

            @Override
            public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
                // There is no strong precedent for how to encode
                // arbitrary bytes in JSON.
                byte[] bytes = source.getBytes();
                String formattedString = options.get(FormatOptions.JsonBinaryFormatOption.class).format(bytes);
                out.append("\"" + formattedString + "\"");
            }
        },
        BIGDECIMAL{
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                if (source.hasCacheValue()) {
                    BigDecimal num = ((BigDecimalWrapper) source.getObject()).asBigDecimal();
                    // toString() uses exponent notation, which SQL reserves for appoximate literals
                    out.append(num.toPlainString());
                }
                else {
                    int precision = type.attribute(DecimalAttribute.PRECISION);
                    int scale = type.attribute(DecimalAttribute.SCALE);
                    ConversionHelperBigDecimal.decodeToString(source.getBytes(), 0, precision, scale, out);
                }
            }

            @Override
            public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
                // The JSON spec just has one kind of number, so we could output with
                // quotes and reserve scientific notation for floats. But almost every
                // library interprets decimal point as floating point,
                // so stick with string.
                out.append('"');
                format(type, source, out);
                out.append('"');
            }
        };

        @Override
        public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
            format(type, source, out);
        }
        
        @Override
        public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
            format(type, source, out);
        }
    }
}
