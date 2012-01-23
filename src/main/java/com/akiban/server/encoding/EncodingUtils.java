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

import com.akiban.server.AkServerUtil;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.persistit.Key;

abstract class EncodingUtils {
    private EncodingUtils() {
    }
    
    /**
     * Writes a VARCHAR or CHAR: inserts the correct-sized PREFIX for MySQL
     * VARCHAR. Assumes US-ASCII encoding, for now. Can be used temporarily for
     * the BLOB types as well.
     *
     * @param obj Object to turn into a string (calls toString)
     * @param bytes Buffer to write bytes into
     * @param offset Position within bytes to write at
     * @param fieldDef Corresponding field
     * @return How many positions in bytes were used
     */
    static int objectToString(final Object obj, final byte[] bytes, final int offset,
                       final FieldDef fieldDef) {
        final String s = obj == null ? "" : obj.toString();
        final byte b[] = stringBytes(s);
        return putByteArray(b, 0, b.length, bytes, offset, fieldDef);
    }

    static int putByteArray(final byte[] src, final int srcOffset, final int srcLength,
                            final byte[] dst, final int dstOffset, final FieldDef fieldDef) {
        int prefixSize = fieldDef.getPrefixSize();
        AkServerUtil.putIntegerByWidth(dst, dstOffset, prefixSize, srcLength);
        System.arraycopy(src, srcOffset, dst, dstOffset + prefixSize, srcLength);
        return prefixSize + srcLength;

    }

    static void toKeyStringEncoding(final FieldDef fieldDef, final RowData rowData,
                             final Key key) {
        final long location = fieldDef.getRowDef().fieldLocation(rowData,
                fieldDef.getFieldIndex());
        key.append(AkServerUtil.decodeMySQLString(rowData.getBytes(),
                (int) location, (int) (location >>> 32), fieldDef));
    }

    static void toKeyByteArrayEncoding(final FieldDef fieldDef, final RowData rowData,
                                final Key key) {
        final long location = fieldDef.getRowDef().fieldLocation(rowData,
                fieldDef.getFieldIndex());
        final int offset = (int) location;
        final int length = (int) (location >>> 32);
        final byte[] bytes;
        if(offset == 0) {
            bytes = null;
        } else {
            bytes = new byte[length - fieldDef.getPrefixSize()];
            System.arraycopy(rowData.getBytes(), offset + fieldDef.getPrefixSize(),
                             bytes, 0, bytes.length);
        }
        key.append(bytes);
    }

    // TODO - This method destroys character encoding

    private static byte[] stringBytes(final String s) {
        final byte[] b = new byte[s.length()];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) s.charAt(i);
        }
        return b;
    }
}
