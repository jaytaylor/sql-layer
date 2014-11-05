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

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.UnsupportedCharsetException;
import com.foundationdb.server.rowdata.FieldDef;

import java.io.UnsupportedEncodingException;

/** General multi-byte byte encoding. Don't know what it will do, so
 * have to go through the full conversion to bytes only to get the
 * length.
*/
public class SlowMBCSEncoder extends VariableWidthEncoding {
    private final String charset;
    
    public SlowMBCSEncoder(String charset) {
        this.charset = charset;
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        int prefixWidth = fieldDef.getPrefixSize();
        if (value == null)
            return prefixWidth;
        else {
            String charsetName = fieldDef.column().getCharsetName();
            try {
                return value.toString().getBytes(charsetName).length + prefixWidth;
            }
            catch (UnsupportedEncodingException ex) {
                TableName table = fieldDef.column().getTable().getName();
                throw new UnsupportedCharsetException(charsetName);
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        return ((other instanceof SlowMBCSEncoder) &&
                charset.equalsIgnoreCase(((SlowMBCSEncoder)other).charset));
    }
}
