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

package com.foundationdb.server.rowdata.encoding;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;
import com.foundationdb.server.types.common.types.StringAttribute;

import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public final class Encoders
{
    private Encoders() {
    }

    public static Encoding encodingFor(TInstance type) {
        TClass tclass = TInstance.tClass(type);
        if (tclass == null)
            return null;
        int jdbcType = tclass.jdbcType();
        switch (jdbcType) {
        case Types.DECIMAL:
        case Types.NUMERIC:
            return DecimalEncoder.INSTANCE;

        case Types.BIGINT:
            if (tclass.isUnsigned())
                return UBigIntEncoder.INSTANCE;
            /* else falls through */
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BOOLEAN:
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
            return LongEncoder.INSTANCE;

        case Types.FLOAT:
        case Types.REAL:
            return FloatEncoder.INSTANCE;
        case Types.DOUBLE:
            return DoubleEncoder.INSTANCE;

        case Types.BINARY:
        case Types.BIT:
        case Types.LONGVARBINARY:
        case Types.VARBINARY:
        case Types.BLOB:
            return VarBinaryEncoder.INSTANCE;

        case Types.CHAR:
        case Types.NCHAR:
        case Types.NVARCHAR:
        case Types.VARCHAR:
        case Types.LONGNVARCHAR:
        case Types.LONGVARCHAR:
        case Types.CLOB:
            return charEncoding(StringAttribute.charsetName(type));

        default:
            if (tclass == AkGUID.INSTANCE) {
                return GuidEncoder.INSTANCE;
            }
            return null;
        }
    }

    private static final Map<String,Encoding> charEncodingMap = new HashMap<>();

    /**
     * Get the encoding for a character column.
     */
    public static Encoding charEncoding(String charsetName) {
        synchronized (charEncodingMap) {
            Encoding encoding = charEncodingMap.get(charsetName);
            if (encoding == null) {
                try {
                    Charset charset = Charset.forName(charsetName);
                    if (charset.name().equals("UTF-8"))
                        encoding = UTF8Encoder.INSTANCE;
                    else if (charset.newEncoder().maxBytesPerChar() == 1.0)
                        encoding = SBCSEncoder.INSTANCE;
                }
                catch (IllegalCharsetNameException |
                       UnsupportedCharsetException |
                       UnsupportedOperationException ex) {
                    encoding = SBCSEncoder.INSTANCE;
                }
                if (encoding == null)
                    encoding = new SlowMBCSEncoder(charsetName);
                charEncodingMap.put(charsetName, encoding);
            }
            return encoding;
        }
    }
}
