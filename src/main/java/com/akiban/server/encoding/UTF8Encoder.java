/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.encoding;

import com.akiban.server.rowdata.FieldDef;

/** Single byte encoding. */
public class UTF8Encoder extends VariableWidthEncoding {

    public static final Encoding INSTANCE = new UTF8Encoder();

    private UTF8Encoder() {
    }

    @Override
    public int widthFromObject(final FieldDef fieldDef, final Object value) {
        int size = fieldDef.getPrefixSize();
        if (value != null) {
            String str = value.toString();
            for (int i = 0; i < str.length(); i++) {
                int ch = str.charAt(i);
                if (ch == '\u0000')
                    size += 2;
                else if (ch <= '\u007F')
                    size += 1;
                else if (ch <= '\u07FF')
                    size += 2;
                else
                    size += 3;
            }
        }
        return size;
    }
}
