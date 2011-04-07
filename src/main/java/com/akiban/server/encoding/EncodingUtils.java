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

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import com.akiban.server.AkServerUtil;
import com.akiban.server.FieldDef;
import com.akiban.server.RowData;
import com.persistit.Key;

abstract class EncodingUtils {
    private EncodingUtils() {
    }
    
    static int objectToInt(final byte[] bytes, final int offset, final Object obj, final int width, final boolean unsigned) {

        final long value;
        if (obj instanceof Number) {
            value = ((Number) obj).longValue();
        } else if (obj instanceof String) {
            value = Long.parseLong((String) obj);
        } else if (obj == null) {
            value = 0;
        } else {
            throw new IllegalArgumentException(obj
                    + " must be a Number or a String");
        }
        if (unsigned) {
            return putUInt(bytes, offset, value, width);
        } else {
            return putInt(bytes, offset, value, width);
        }
    }

    public static int putInt(final byte[] bytes, final int offset, final long value,
               final int width) {
        switch (width) {
            case 1:
                AkServerUtil.putByte(bytes, offset, (byte) value);
                break;
            case 2:
                AkServerUtil.putShort(bytes, offset, (short) value);
                break;
            case 3:
                AkServerUtil.putMediumInt(bytes, offset, (int) value);
                break;
            case 4:
                AkServerUtil.putInt(bytes, offset, (int) value);
                break;
            case 8:
                AkServerUtil.putLong(bytes, offset, value);
                break;
            default:
                throw new IllegalStateException("Width not supported");
        }
        return width;
    }

    static int putUInt(final byte[] bytes, final int offset, final long value,
                final int width) {
        switch (width) {
            case 1:
                AkServerUtil.putByte(bytes, offset, (int) (value < 0 ? 0 : value));
                break;
            case 2:
                AkServerUtil.putShort(bytes, offset, (int) (value < 0 ? 0 : value));
                break;
            case 3:
                AkServerUtil.putMediumInt(bytes, offset, (int) (value < 0 ? 0
                        : value));
                break;
            case 4:
                AkServerUtil.putInt(bytes, offset, (int) (value < 0 ? 0 : value));
                break;
            case 8:
                AkServerUtil.putLong(bytes, offset, value);
                break;
            default:
                throw new IllegalStateException("Width not supported");
        }
        return width;
    }



    /**
     * Writes a VARCHAR or CHAR: inserts the correct-sized PREFIX for MySQL
     * VARCHAR. Assumes US-ASCII encoding, for now. Can be used temporarily for
     * the BLOB types as well.
     *
     * @param obj
     * @param bytes
     * @param offset
     * @param fieldDef
     * @return
     */
    static int objectToString(final Object obj, final byte[] bytes, final int offset,
                       final FieldDef fieldDef) {
        final String s = obj == null ? "" : obj.toString();
        return putByteArray(stringBytes(s), bytes, offset, fieldDef);
    }


    static int objectToFloat(final byte[] bytes, final int offset, final Object obj, final boolean unsigned) {
        float f;
        if (obj instanceof Number) {
            f = ((Number) obj).floatValue();
        } else if (obj instanceof String) {
            f = Float.parseFloat((String) obj);
        } else if (obj == null) {
            f = 0f;
        } else
            throw new IllegalArgumentException(obj
                    + " must be a Number or a String");
        if (unsigned) {
            f = Math.max(0f, f);
        }
        return putInt(bytes, offset, Float.floatToIntBits(f), 4);
    }

    static int objectToDouble(final byte[] bytes, final int offset, final Object obj, final boolean unsigned) {
        double d;
        if (obj instanceof Number) {
            d = ((Number) obj).doubleValue();
        } else if (obj instanceof String) {
            d = Double.parseDouble((String) obj);
        } else if (obj == null) {
            d = 0d;
        } else
            throw new IllegalArgumentException(obj
                    + " must be a Number or a String");
        if (unsigned) {
            d = Math.max(0d, d);
        }
        return putInt(bytes, offset, Double.doubleToLongBits(d), 8);
    }


    public final static String SDF_DATE = "yyyy-MM-dd";


    public final static String SDF_TIME = "HH:mm:ss";

    private static ThreadLocal<Map<String, SimpleDateFormat>> SDF_MAP_THREAD_LOCAL = new ThreadLocal<Map<String, SimpleDateFormat>>();

    static SimpleDateFormat getDateFormat(final String pattern) {
        Map<String, SimpleDateFormat> formatMap = SDF_MAP_THREAD_LOCAL.get();
        if (formatMap == null) {
            formatMap = new HashMap<String, SimpleDateFormat>();
            SDF_MAP_THREAD_LOCAL.set(formatMap);
        }
        SimpleDateFormat sdf = formatMap.get(pattern);
        if (sdf == null) {
            sdf = new SimpleDateFormat(pattern);
            formatMap.put(pattern, sdf);
        } else {
            sdf.getCalendar().clear();
        }
        return sdf;
    }

    static int putByteArray(final byte[] b, final byte[] bytes, final int offset,
                     final FieldDef fieldDef) {
        int prefixSize = fieldDef.getPrefixSize();
        final int size = b.length;
        switch (prefixSize) {
            case 0:
                break;
            case 1:
                AkServerUtil.putByte(bytes, offset, size);
                break;
            case 2:
                AkServerUtil.putChar(bytes, offset, size);
                break;
            case 3:
                AkServerUtil.putMediumInt(bytes, offset, size);
                break;
            case 4:
                AkServerUtil.putInt(bytes, offset, size);
                break;
            default:
                throw new Error("Missing case");
        }
        System.arraycopy(b, 0, bytes, offset + prefixSize, size);

        return prefixSize + size;

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
        final byte[] bytes = new byte[length - fieldDef.getPrefixSize()];
        System.arraycopy(rowData.getBytes(), offset + fieldDef.getPrefixSize(),
                bytes, 0, bytes.length);
        key.append(bytes);
    }

    // TODO -
    // These methods destroy character encoding - I added them just to get over
    // a unit test problem in loading xxxxxxxx data in which actual data is
    // loaded.
    // We will need to implement character encoding properly to handle
    // xxxxxxxx data properly.
    //

    static int stringByteLength(final String s) {
        return s.length();
    }

    private static byte[] stringBytes(final String s) {
        final byte[] b = new byte[s.length()];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) s.charAt(i);
        }
        return b;
    }
}
