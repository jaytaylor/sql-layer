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

package com.foundationdb.server.types3.common;

import com.foundationdb.server.rowdata.ConversionHelperBigDecimal;
import com.foundationdb.server.types3.TClassFormatter;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.common.types.StringAttribute;
import com.foundationdb.server.types3.common.types.StringFactory;
import com.foundationdb.server.types3.mcompat.mtypes.MBigDecimal.Attrs;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.util.AkibanAppender;
import com.google.common.primitives.UnsignedLongs;
import org.apache.commons.codec.binary.Base64;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.Formatter;

public class NumericFormatter {

    public static enum FORMAT implements TClassFormatter {
        FLOAT {
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append(Float.toString(source.getFloat()));
            }

            @Override
            public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
                new Formatter(out.getAppendable()).format("%e", source.getFloat());
            }
        },
        DOUBLE {
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append(Double.toString(source.getDouble()));
            }

            @Override
            public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
                new Formatter(out.getAppendable()).format("%e", source.getDouble());
            }
        },
        INT_8 {
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append(Byte.toString(source.getInt8()));
            }
        },
        INT_16 {
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append(Short.toString(source.getInt16()));
            }
        },
        INT_32 {
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append(Integer.toString(source.getInt32()));
            }
        },
        INT_64 {
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append(Long.toString(source.getInt64()));
            }
        },
        UINT_64 {
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                out.append(UnsignedLongs.toString(source.getInt64()));
            }
        },
        BYTES {
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                String charsetName = StringFactory.DEFAULT_CHARSET.name();
                Charset charset = Charset.forName(charsetName);
                String str = new String(source.getBytes(), charset);
                out.append(str);
            }

            @Override
            public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
                byte[] value = source.getBytes();
                out.append("X'");
                for (int i = 0; i < value.length; i++) {
                    int b = value[i] & 0xFF;
                    out.append(hexDigit(b >> 4));
                    out.append(hexDigit(b & 0xF));
                }
                out.append('\'');
            }

            private char hexDigit(int n) {
                if (n < 10)
                    return (char)('0' + n);
                else
                    return (char)('A' + n - 10);
            }

            @Override
            public void formatAsJson(TInstance instance, PValueSource source, AkibanAppender out) {
                // There is no strong precedent for how to encode
                // arbitrary bytes in JSON.
                out.append('"');
                out.append(Base64.encodeBase64String(source.getBytes()));
                out.append('"');
            }
        },
        BIGDECIMAL{
            @Override
            public void format(TInstance instance, PValueSource source, AkibanAppender out) {
                if (source.hasCacheValue()) {
                    BigDecimal num = ((BigDecimalWrapper) source.getObject()).asBigDecimal();
                    out.append(num.toString());
                }
                else {
                    int precision = instance.attribute(Attrs.PRECISION);
                    int scale = instance.attribute(Attrs.SCALE);
                    ConversionHelperBigDecimal.decodeToString(source.getBytes(), 0, precision, scale, out);
                }
            }

            @Override
            public void formatAsJson(TInstance instance, PValueSource source, AkibanAppender out) {
                // The JSON spec just has one kind of number, so we could output with
                // quotes and reserve scientific notation for floats. But almost every
                // library interprets decimal point as floating point,
                // so stick with string.
                out.append('"');
                format(instance, source, out);
                out.append('"');
            }
        };

        @Override
        public void formatAsLiteral(TInstance instance, PValueSource source, AkibanAppender out) {
            format(instance, source, out);
        }
        
        @Override
        public void formatAsJson(TInstance instance, PValueSource source, AkibanAppender out) {
            format(instance, source, out);
        }
    }
}
