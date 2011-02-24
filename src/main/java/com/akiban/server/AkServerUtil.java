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

package com.akiban.server;

import com.akiban.util.AkibanAppender;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class AkServerUtil {

    private final static boolean BIG_ENDIAN = false;

    private final static char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5',
            '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

    public final static String NEW_LINE = System.getProperty("line.separator");

    public static long getSignedIntegerByWidth(final byte[] bytes,
            final int index, final int width) {
        switch (width) {
        case 0:
            return 0;
        case 1:
            return (byte) bytes[index];
        case 2:
            return (short) getShort(bytes, index);
        case 3:
            return getMediumInt(bytes, index);
        case 4:
            return (int) getInt(bytes, index);
        case 8:
            return getLong(bytes, index);
        default:
            throw new IllegalArgumentException(
                    "Width must be 0,2,3,4 or 8 but is: " + width);
        }
    }

    public static long getUnsignedIntegerByWidth(final byte[] bytes,
            final int index, final int width) {
        switch (width) {
        case 0:
            return 0;
        case 1:
            return getByte(bytes, index) & 0xFF;
        case 2:
            return getChar(bytes, index) & 0xFFFF;
        case 3:
            return getMediumInt(bytes, index) & 0xFFFFFF;
        case 4:
            return getInt(bytes, index) & 0xFFFFFFFF;
        case 8:
            return getLong(bytes, index); // TODO
            // throw new UnsupportedOperationException(
            // "Currently can't handle unsigned 64-bit integers");
        default:
            throw new IllegalArgumentException(
                    "Width must be 0,1,2,3,4 or 8 but is: " + width);
        }
    }

    public static int getByte(byte[] bytes, int index) {
        return (bytes[index + 0] & 0xFF);
    }

    public static int getShort(byte[] bytes, int index) {
        if (BIG_ENDIAN) {
            return (short) ((bytes[index + 1] & 0xFF) | (bytes[index + 0]) << 8);
        } else {
            return (short) ((bytes[index + 0] & 0xFF) | (bytes[index + 1]) << 8);
        }
    }

    public static int getChar(byte[] bytes, int index) {
        if (BIG_ENDIAN) {
            return (bytes[index + 1] & 0xFF) | (bytes[index + 0] & 0xFF) << 8;
        } else {
            return (bytes[index + 0] & 0xFF) | (bytes[index + 1] & 0xFF) << 8;
        }
    }

    public static int getMediumInt(byte[] bytes, int index) {
        if (BIG_ENDIAN) {
            return (bytes[index + 2] & 0xFF) | (bytes[index + 1] & 0xFF) << 8
                    | (bytes[index + 0] & 0xFF) << 16;
        } else {
            return (bytes[index + 0] & 0xFF) | (bytes[index + 1] & 0xFF) << 8
                    | (bytes[index + 2] & 0xFF) << 16;
        }
    }

    public static int getInt(byte[] bytes, int index) {
        if (BIG_ENDIAN) {
            return (bytes[index + 3] & 0xFF) | (bytes[index + 2] & 0xFF) << 8
                    | (bytes[index + 1] & 0xFF) << 16
                    | (bytes[index + 0] & 0xFF) << 24;
        } else {
            return (bytes[index + 0] & 0xFF) | (bytes[index + 1] & 0xFF) << 8
                    | (bytes[index + 2] & 0xFF) << 16
                    | (bytes[index + 3] & 0xFF) << 24;
        }
    }

    public static long getLong(byte[] bytes, int index) {
        if (BIG_ENDIAN) {
            return (long) (bytes[index + 7] & 0xFF)
                    | (long) (bytes[index + 6] & 0xFF) << 8
                    | (long) (bytes[index + 5] & 0xFF) << 16
                    | (long) (bytes[index + 4] & 0xFF) << 24
                    | (long) (bytes[index + 3] & 0xFF) << 32
                    | (long) (bytes[index + 2] & 0xFF) << 40
                    | (long) (bytes[index + 1] & 0xFF) << 48
                    | (long) (bytes[index + 0] & 0xFF) << 56;
        } else {
            return (long) (bytes[index + 0] & 0xFF)
                    | (long) (bytes[index + 1] & 0xFF) << 8
                    | (long) (bytes[index + 2] & 0xFF) << 16
                    | (long) (bytes[index + 3] & 0xFF) << 24
                    | (long) (bytes[index + 4] & 0xFF) << 32
                    | (long) (bytes[index + 5] & 0xFF) << 40
                    | (long) (bytes[index + 6] & 0xFF) << 48
                    | (long) (bytes[index + 7] & 0xFF) << 56;
        }
    }

    public static float getFloat(byte[] bytes, int index) {
        return Float.intBitsToFloat(getInt(bytes, index));
    }

    public static double getDouble(byte[] bytes, int index) {
        return Double.longBitsToDouble(getLong(bytes, index));
    }

    public static int putByte(byte[] bytes, int index, int value) {
        bytes[index] = (byte) (value);
        return index + 1;
    }

    public static int putShort(byte[] bytes, int index, int value) {
        if (BIG_ENDIAN) {
            bytes[index + 1] = (byte) (value);
            bytes[index + 0] = (byte) (value >>> 8);
        } else {
            bytes[index + 0] = (byte) (value);
            bytes[index + 1] = (byte) (value >>> 8);
        }
        return index + 2;
    }

    public static int putChar(byte[] bytes, int index, int value) {
        if (BIG_ENDIAN) {
            bytes[index + 1] = (byte) (value);
            bytes[index + 0] = (byte) (value >>> 8);
        } else {
            bytes[index + 0] = (byte) (value);
            bytes[index + 1] = (byte) (value >>> 8);
        }
        return index + 2;
    }

    public static int putMediumInt(byte[] bytes, int index, int value) {
        if (BIG_ENDIAN) {
            bytes[index + 2] = (byte) (value);
            bytes[index + 1] = (byte) (value >>> 8);
            bytes[index + 0] = (byte) (value >>> 16);
        } else {
            bytes[index + 0] = (byte) (value);
            bytes[index + 1] = (byte) (value >>> 8);
            bytes[index + 2] = (byte) (value >>> 16);
        }
        return index + 3;
    }

    public static int putInt(byte[] bytes, int index, int value) {
        if (BIG_ENDIAN) {
            bytes[index + 3] = (byte) (value);
            bytes[index + 2] = (byte) (value >>> 8);
            bytes[index + 1] = (byte) (value >>> 16);
            bytes[index + 0] = (byte) (value >>> 24);
        } else {
            bytes[index + 0] = (byte) (value);
            bytes[index + 1] = (byte) (value >>> 8);
            bytes[index + 2] = (byte) (value >>> 16);
            bytes[index + 3] = (byte) (value >>> 24);
        }
        return index + 4;
    }

    public static int putLong(byte[] bytes, int index, long value) {
        if (BIG_ENDIAN) {
            bytes[index + 7] = (byte) (value);
            bytes[index + 6] = (byte) (value >>> 8);
            bytes[index + 5] = (byte) (value >>> 16);
            bytes[index + 4] = (byte) (value >>> 24);
            bytes[index + 3] = (byte) (value >>> 32);
            bytes[index + 2] = (byte) (value >>> 40);
            bytes[index + 1] = (byte) (value >>> 48);
            bytes[index + 0] = (byte) (value >>> 56);
        } else {
            bytes[index + 0] = (byte) (value);
            bytes[index + 1] = (byte) (value >>> 8);
            bytes[index + 2] = (byte) (value >>> 16);
            bytes[index + 3] = (byte) (value >>> 24);
            bytes[index + 4] = (byte) (value >>> 32);
            bytes[index + 5] = (byte) (value >>> 40);
            bytes[index + 6] = (byte) (value >>> 48);
            bytes[index + 7] = (byte) (value >>> 56);
        }
        return index + 8;
    }

    public static int putFloat(byte[] bytes, int index, float value) {
        return putInt(bytes, index, Float.floatToIntBits(value));
    }

    public static int putDouble(byte[] bytes, int index, double value) {
        return putLong(bytes, index, Double.doubleToLongBits(value));
    }

    public static int putBytes(byte[] bytes, int index, byte[] value) {
        System.arraycopy(value, 0, bytes, index, value.length);
        return index + value.length;
    }

    public static int putBytes(byte[] bytes, int index, byte[] value,
            int offset, int length) {
        System.arraycopy(value, offset, bytes, index, length);
        return index + length;
    }

    public static String dump(char[] c, int offset, int size) {
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        for (int m = 0; m < size - offset; m += 8) {
            sb2.setLength(0);
            hex(sb1, m, 4);
            sb1.append(":");
            for (int i = 0; i < 8; i++) {
                sb1.append("  ");
                if (i % 4 == 0)
                    sb1.append(" ");
                int j = m + i;
                if (j < size - offset) {
                    hex(sb1, c[j + offset], 4);
                    if (c[j + offset] >= 32 && c[j] < 127)
                        sb2.append(c[j]);
                    else
                        sb2.append(".");
                } else
                    sb1.append("    ");
            }
            sb1.append("    ");
            sb1.append(sb2.toString());
            sb1.append(NEW_LINE);
        }
        return sb1.toString();
    }

    public static String dump(byte[] b, int offset, int size) {
        StringBuilder sb1 = new StringBuilder();
        StringBuilder sb2 = new StringBuilder();
        for (int m = 0; m < size; m += 16) {
            sb2.setLength(0);
            hex(sb1, m, 4);
            sb1.append(":");
            for (int i = 0; i < 16; i++) {
                sb1.append(" ");
                if (i % 8 == 0)
                    sb1.append(" ");
                int j = m + i;
                if (j < size) {
                    hex(sb1, b[j + offset], 2);
                    final char c = (char) (b[j + offset] & 0xFF);
                    sb2.append(c > 32 && c < 127 ? c : '.');
                } else
                    sb1.append("  ");
            }
            sb1.append("  ");
            sb1.append(sb2.toString());
            sb1.append(NEW_LINE);
        }
        return sb1.toString();
    }

    public static String hex(byte[] bytes, int start, int length) {
        final StringBuilder sb = new StringBuilder(length * 2);
        hex(AkibanAppender.of(sb), bytes, start, length);
        return sb.toString();
    }

    public static StringBuilder hex(StringBuilder sb, long value, int length) {
        for (int i = length - 1; i >= 0; i--) {
            sb.append(HEX_DIGITS[(int) (value >> (i * 4)) & 0xF]);
        }
        return sb;
    }

    public static void hex(AkibanAppender sb, byte[] bytes, int start,
            int length) {
        for (int i = start; i < start + length; i++) {
            sb.append(HEX_DIGITS[(bytes[i] & 0xF0) >>> 4]);
            sb.append(HEX_DIGITS[(bytes[i] & 0x0F)]);
        }
    }

    public static void printRuntimeInfo() {
        System.out.println();
        RuntimeMXBean m = java.lang.management.ManagementFactory
                .getRuntimeMXBean();
        System.out.println("BootClassPath = " + m.getBootClassPath());
        System.out.println("ClassPath = " + m.getClassPath());
        System.out.println("LibraryPath = " + m.getLibraryPath());
        System.out.println("ManagementSpecVersion = "
                + m.getManagementSpecVersion());
        System.out.println("Name = " + m.getName());
        System.out.println("SpecName = " + m.getSpecName());
        System.out.println("SpecVendor = " + m.getSpecVendor());
        System.out.println("SpecVersion = " + m.getSpecVersion());
        System.out.println("UpTime = " + m.getUptime());
        System.out.println("VmName = " + m.getVmName());
        System.out.println("VmVendor = " + m.getVmVendor());
        System.out.println("VmVersion = " + m.getVmVersion());
        System.out.println("InputArguments = " + m.getInputArguments());
        System.out.println("BootClassPathSupported = "
                + m.isBootClassPathSupported());
        System.out.println("---all properties--");
        System.out.println("SystemProperties = " + m.getSystemProperties());
        System.out.println("---");
        System.out.println();
    }

    public final static void cleanUpDirectory(final File file) {
        if (!file.exists()) {
            file.mkdirs();
            return;
        } else if (file.isFile()) {
            throw new IllegalStateException(file + " must be a directory");
        } else {
            final File[] files = file.listFiles();
            if (files != null) {
                cleanUpFiles(files);
            }
        }
    }

    public final static void cleanUpFiles(final File[] files) {
        for (final File file : files) {
            if (file.isDirectory()) {
                cleanUpDirectory(file);
            }
            file.delete();
        }
    }

    /**
     * Cracks the MySQL variable-length format. Interprets 0, 1, 2 or 3 prefix
     * bytes as a little-endian string size and constructs a string from the
     * remaining bytes.
     * 
     * TODO: does not handle non US-ASCII character sets.
     * 
     * @param bytes
     * @param offset
     * @param width
     * @return
     */
    public static String decodeMySQLString(byte[] bytes, final int offset,
            final int width, final FieldDef fieldDef) {
        ByteBuffer buff = byteBufferForMySQLString(bytes, offset, width, fieldDef);
        return buff == null ? null
            // TODO - handle char set, e.g., utf8
            : new String(buff.array(), buff.position(), buff.limit() - buff.position());
    }

    public static ByteBuffer byteBufferForMySQLString(byte[] bytes, final int offset,
                                           final int width, final FieldDef fieldDef) {
        if (width == 0) {
            return null;
        }

        final int prefixSize = fieldDef.getPrefixSize();
        int length;
        switch (prefixSize) {
            case 0:
                length = 0;
                break;
            case 1:
                length = getByte(bytes, offset);
                break;
            case 2:
                length = getChar(bytes, offset);
                break;
            case 3:
                length = getMediumInt(bytes, offset);
                break;
            case 4:
                length = getInt(bytes, offset);
                break;
            default:
                throw new Error("No such case");
        }
        if (length > width) {
            throw new IllegalArgumentException(
                    "String is wider than available bytes: " + length);
        }

        return ByteBuffer.wrap(bytes, offset + prefixSize, width - prefixSize);
    }

    public static int varWidth(final int length) {
        return length == 0 ? 0 : length < 0x100 ? 1 : length < 0x10000 ? 2
                : length < 0x1000000 ? 3 : 4;
    }

    public static long availableMemory() {
        final MemoryUsage mu = ManagementFactory.getMemoryMXBean()
                .getHeapMemoryUsage();
        long max = mu.getMax();
        if (max == -1) {
            max = mu.getInit();
        }
        return max;
    }
    
    public static boolean equals(final Object a, final Object b) {
        return a == null ? b == null : a.equals(b);
    }
    
    public static int hashCode(final Object o) {
        return o == null ? Integer.MIN_VALUE : o.hashCode();
    }
}
