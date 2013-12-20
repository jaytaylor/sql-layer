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

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.rowdata.FieldDef;

import java.io.UnsupportedEncodingException;

/** Single byte encoding. */
public class UTF8Encoder extends VariableWidthEncoding {

    public static final Encoding INSTANCE = new UTF8Encoder();

    // See https://tools.ietf.org/html/rfc3629
    private static final int MAX_1_BYTE = 0x007F;
    private static final int MAX_2_BYTE = 0x07FF;
    private static final int MAX_3_BYTE = 0xFFFF;
    private static final int MAX_4_BYTE = 0x10FFFF;

    private UTF8Encoder() {
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        int size = fieldDef.getPrefixSize();
        if (value != null) {
            String str;
            if (value instanceof byte[]) {
                try {
                    str = new String((byte[]) value, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new AkibanInternalException("while decoding binary", e);
                }
            }
            else {
                str = value.toString();
            }
            for (int i = 0; i < str.length(); i++) {
                int ch = str.charAt(i);
                // Assumes consumers want standard UTF8 (e.g. String, nio.charset), not modified
                if (ch <= MAX_1_BYTE)
                    size += 1;
                else if (ch <= MAX_2_BYTE)
                    size += 2;
                else {
                    // codePointAt will return the same as charAt if not a high surrogate pair *or* not followed by low
                    int codePoint = str.codePointAt(i);
                    if (codePoint == ch) {
                        size += 3;
                    } else {
                        if (++i >= str.length())
                            throw new IllegalStateException("Got codePoint but missing low pair: " + str);
                        if (codePoint <= MAX_3_BYTE)
                            size += 3;
                        else {
                            assert codePoint <= MAX_4_BYTE : "Illegal code point: " + codePoint;
                            size += 4;
                        }
                    }
                }
            }
        }
        return size;
    }
}
