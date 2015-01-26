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

import com.foundationdb.server.service.blob.BlobRef;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.TClassFormatter;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.common.types.StringFactory;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.AkibanAppender;
import com.foundationdb.util.Strings;
import java.nio.charset.Charset;
import java.util.UUID;

public class TFormatter {

    public static enum FORMAT implements TClassFormatter {
        BOOL {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(Boolean.toString(source.getBoolean()));
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(source.getBoolean() ? "TRUE" : "FALSE");
            }

            @Override
            public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
                format(type, source, out);
            }
        },
        GUID {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                out.append(((UUID) source.getObject()).toString());
            }
    
            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                out.append("'");
                format(type, source, out);
                out.append("'");
            }
    
            @Override
            public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
                out.append("\"");
                format(type, source, out);
                out.append("\"");
            }
        },
        BLOB {
            @Override
            public void format(TInstance type, ValueSource source, AkibanAppender out) {
                BlobRef blob = (BlobRef) source.getObject();
                if (blob.isShortLob()) {
                    String charsetName = StringFactory.DEFAULT_CHARSET.name();
                    Charset charset = Charset.forName(charsetName);
                    String str = new String(blob.getIdOrBytes(), charset);
                    out.append(str);
                } 
                else if (blob.isLongLob()) {
                    out.append(blob.getId().toString());
                } 
            }

            @Override
            public void formatAsLiteral(TInstance type, ValueSource source, AkibanAppender out) {
                BlobRef blob = (BlobRef) source.getObject();
                if (blob.isShortLob()) {
                    byte[] value = source.getBytes();
                    out.append("X'");
                    out.append(Strings.hex(value));
                    out.append("'");
                }
                else if (blob.isLongLob()) {
                    out.append("'");
                    format(type, source, out);
                    out.append("'");
                }
            }

            @Override
            public void formatAsJson(TInstance type, ValueSource source, AkibanAppender out, FormatOptions options) {
                BlobRef blob = (BlobRef) source.getObject();
                if (blob.isShortLob()) {
                    String formattedString = options.get(FormatOptions.JsonBinaryFormatOption.class).format(blob.getBytes());
                    out.append("\"" + formattedString + "\"");
                }
                else if (blob.isLongLob()) {
                    out.append("\"");
                    format(type, source, out);
                    out.append("\"");
                }
            }
        };
    }
    
    
}
