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

import com.akiban.ais.model.TableName;
import com.akiban.server.error.UnsupportedCharsetException;
import com.akiban.server.rowdata.FieldDef;

import java.io.UnsupportedEncodingException;

/** Single byte encoding. */
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
            String charsetName = fieldDef.column().getCharsetAndCollation().charset();
            try {
                return value.toString().getBytes(charsetName).length + prefixWidth;
            }
            catch (UnsupportedEncodingException ex) {
                TableName table = fieldDef.column().getTable().getName();
                throw new UnsupportedCharsetException(table.getSchemaName(), table.getTableName(), charsetName);
            }
        }
    }
}
