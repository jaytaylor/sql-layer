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

package com.foundationdb.tuple;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.nio.charset.Charset;

import com.foundationdb.tuple.TupleUtil.DecodeResult;
import com.foundationdb.util.WrappingByteSource;

/**
 * 
 * Utility functions for encoding/decoding tuples.
 *
 */
class TupleFloatingUtil {
    
    private static final byte nil = 0x0;

    static final int FLOAT_LEN = 4;
    static final int DOUBLE_LEN = 8;
    static final int INT_LEN = 4;
    static final int UUID_LEN = 16;

    static final byte FLOAT_CODE = 0x20;
    static final byte DOUBLE_CODE = 0x21;
    static final byte BIGINT_NEG_CODE = 0x1d;
    static final byte BIGINT_POS_CODE = 0x1e;
    static final byte BIGDEC_NEG_CODE = 0x23;
    static final byte BIGDEC_POS_CODE = 0x24;
    static final byte TRUE_CODE = 0x25;
    static final byte FALSE_CODE = 0x26;
    static final byte UUID_CODE = 0x30;

    static byte[] floatingPointToByteArray (float value) {
        return ByteBuffer.allocate(FLOAT_LEN).putFloat(value).order(ByteOrder.BIG_ENDIAN).array();
    }

    static byte[] floatingPointToByteArray(double value) {
        return ByteBuffer.allocate(DOUBLE_LEN).putDouble(value).order(ByteOrder.BIG_ENDIAN).array();
    }

    static float byteArrayToFloat(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getFloat();
    }

    static double byteArrayToDouble(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getDouble();
    }

    /**
     * For encoding: if the sign bit is 1, flips all bits in the {@code byte[]};
     * else, just flips the sign bit.
     * <br><br>
     * For decoding: if the sign bit is 1, flips all bits in the {@code byte[]};
     * else, just flips the sign bit.
     * 
     * @param bytes - a Big-Endian IEEE binary representation of float, double, or BigInteger
     * @param encode - if true, encodes; if false, decodes
     * @return the encoded {@code byte[]}
     */
    static byte[] floatingPointCoding(byte[] bytes, boolean encode) {
        if (encode && (bytes[0] & (byte) 0x80) != (byte) 0x00) {
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) (bytes[i] ^ 0xff);
            }
        }
        else if (!encode && (bytes[0] & (byte) 0x80) != (byte) 0x80) {
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) (bytes[i] ^ 0xff);
            }
        }
        else {
            bytes[0] = (byte) (0x80 ^ bytes[0]); 
        }

        return bytes;
    }

    static byte[] encode(Object t) {
        if(t == null)
            return new byte[] {nil};
        if (t instanceof UUID)
             return encode((UUID) t);
        if(t instanceof byte[])
            return TupleUtil.encode((byte[]) t);
        if(t instanceof WrappingByteSource)
            return TupleUtil.encode(((WrappingByteSource)t).byteArray());
        if(t instanceof String)
            return TupleUtil.encode((String) t);
        if (t instanceof Float)
            return encode((Float) t);
        if (t instanceof Double)
            return encode((Double) t);
        if (t instanceof BigDecimal)
            return encode((BigDecimal) t);
        if (t instanceof BigInteger)
            return encode((BigInteger) t);
        if (t instanceof Number) 
            return TupleUtil.encode(((Number)t).longValue());
        if (t instanceof Boolean)
            return encode((Boolean) t);
        throw new IllegalArgumentException("Unsupported data type: " + t.getClass().getName());
    }

    static byte[] encode(Float value) {
        byte[] bytes = floatingPointToByteArray(value);
        bytes = floatingPointCoding(bytes, true);   
        byte[] typecode = {FLOAT_CODE};
        return ByteArrayUtil.join(typecode, bytes);
    }

    static byte[] encode(Double value) {
        byte[] bytes = floatingPointToByteArray(value);
        bytes = floatingPointCoding(bytes, true);
        byte[] typecode =  {DOUBLE_CODE};
        return ByteArrayUtil.join(typecode, bytes);
    }

    static byte[] encode(UUID value) {
        return ByteBuffer.allocate(1+UUID_LEN).put(UUID_CODE).order(ByteOrder.BIG_ENDIAN)
                 .putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits())
                 .array();
        }

    static byte[] encode(BigInteger value) {
        byte[] bigIntBytes = encodeBigIntNoTypeCode(value);
        byte[] typecode = {BIGINT_POS_CODE};
        if (value.compareTo(BigInteger.ZERO) < 0) {
        	typecode[0] = BIGINT_NEG_CODE;
        }
        byte[] length = encodeIntNoTypeCode(bigIntBytes.length);
        return ByteArrayUtil.join(typecode, length, bigIntBytes);
    }

    static byte[] encode(BigDecimal value) {
        byte[] bigIntBytes = encodeBigIntNoTypeCode(value.unscaledValue());
        byte[] scaleBytes = encodeIntNoTypeCode(value.scale());
        byte[] typecode = {BIGDEC_POS_CODE}; 
        if (value.compareTo(BigDecimal.ZERO)< 0) {
        	typecode[0] = BIGDEC_NEG_CODE;
        }
        byte[] length = encodeIntNoTypeCode(bigIntBytes.length);
        return ByteArrayUtil.join(typecode, scaleBytes, length, bigIntBytes);
    }

    static byte[] encode(Boolean value) {
        byte[] encoded = {value ? TRUE_CODE : FALSE_CODE};
        return encoded;
    }

    static DecodeResult decodeFloat(byte[] bytes, int start) {
        int end = start + FLOAT_LEN;
        bytes = floatingPointCoding(Arrays.copyOfRange(bytes, start, start + FLOAT_LEN), false);
        return new DecodeResult(end, byteArrayToFloat(bytes));
    }

    static DecodeResult decodeUUID(byte[] bytes, int start) {
            ByteBuffer bb = ByteBuffer.wrap(bytes, start, UUID_LEN).order(ByteOrder.BIG_ENDIAN);
            long msb = bb.getLong();
            long lsb = bb.getLong();
            return new DecodeResult(start + UUID_LEN, new UUID(msb, lsb));
        }    
    
    static DecodeResult decodeDouble(byte[] bytes, int start) {
        int end = start + DOUBLE_LEN;
        bytes = floatingPointCoding(Arrays.copyOfRange(bytes, start, end), false);
        return new DecodeResult(end, byteArrayToDouble(bytes));
    }

    static DecodeResult decodeBigInt(byte[] bytes, int start) {
        int length = decodeIntNoTypeCode(Arrays.copyOfRange(bytes, start, start + INT_LEN));
        BigInteger bigInt = decodeBigIntNoTypeCode(Arrays.copyOfRange(bytes, start + INT_LEN, start + INT_LEN + length));
        return new DecodeResult(start + INT_LEN + length, bigInt);
    }

    static DecodeResult decodeBigDecimal(byte[] bytes, int start) {
        int scale = decodeIntNoTypeCode(Arrays.copyOfRange(bytes, start, start + INT_LEN));
        int length = decodeIntNoTypeCode(Arrays.copyOfRange(bytes, start + INT_LEN, start + INT_LEN * 2));
        BigInteger bigInt = decodeBigIntNoTypeCode(Arrays.copyOfRange(bytes, start + INT_LEN * 2, start + INT_LEN * 2 + length));
        return new DecodeResult(start + INT_LEN * 2 + length, new BigDecimal(bigInt, scale));
    }

    static byte[] encodeBigIntNoTypeCode(BigInteger value) {
        byte[] bytes = value.toByteArray();
        return floatingPointCoding(bytes, true);
    }

    static BigInteger decodeBigIntNoTypeCode(byte[] bytes) {
        bytes = floatingPointCoding(bytes, false);
        return new BigInteger(bytes);
    }

    static byte[] encodeIntNoTypeCode(int i) {
        return ByteBuffer.allocate(INT_LEN).order(ByteOrder.LITTLE_ENDIAN).putInt(i).array();
    }

    static int decodeIntNoTypeCode(byte[] bytes) {
        if(bytes.length != INT_LEN) {
            throw new IllegalArgumentException("Source array must be of length "+String.valueOf((INT_LEN)));
        }
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }
    
    static List<Object> unpack(byte[] bytes, int start, int length) {
        List<Object> items = new LinkedList<Object>();
        int pos = start;
        int end = start + length;
        while(pos < bytes.length) {
            DecodeResult decoded = decode(bytes, pos, end);
            items.add(decoded.o);
            pos = decoded.end;
        }
        return items;
    }

    static byte[] pack(List<Object> items) {
        if(items.size() == 0)
            return new byte[0];

        List<byte[]> parts = new ArrayList<byte[]>(items.size());
        for(Object t : items) {
            //System.out.println("Starting encode: " + ArrayUtils.printable((byte[])t));
            byte[] encoded = encode(t);
            //System.out.println(" encoded -> '" + ArrayUtils.printable(encoded) + "'");
            parts.add(encoded);
        }
        //System.out.println("Joining whole tuple...");
        return ByteArrayUtil.join(null, parts);
    }

    static DecodeResult decode(byte[] rep, int pos, int last) {
        //System.out.println("Decoding '" + ArrayUtils.printable(rep) + "' at " + pos);

        // SOMEDAY: codes over 127 will be a problem with the signed Java byte mess
        int code = rep[pos];
        int start = pos + 1;
        if(code >= 0x0 && code <= 0x2 || code >= 12 && code <= 28) {
            return TupleUtil.decode(rep, pos, last);
        }
        if (code == UUID_CODE) {
            return decodeUUID(rep, start);
        }
        if (code == FLOAT_CODE) {
            return decodeFloat(rep, start);
        }
        if (code == DOUBLE_CODE) {
            return decodeDouble(rep, start);
        }
        if (code == TRUE_CODE) {
            return new DecodeResult(start, true);
        }
        if (code == FALSE_CODE) {
            return new DecodeResult(start, false);
        }
        if (code == BIGDEC_POS_CODE || code == BIGDEC_NEG_CODE) {
            return decodeBigDecimal(rep, start);
        }
        if (code == BIGINT_POS_CODE || code == BIGINT_NEG_CODE) {
            return decodeBigInt(rep, start);
        }
        throw new IllegalArgumentException("Unknown tuple data type " + code + " at index " + pos);
    }
}










