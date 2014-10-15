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

package com.foundationdb.util;

import org.junit.Test;

import java.nio.BufferOverflowException;
import java.nio.ByteOrder;
import java.util.Arrays;

import static com.foundationdb.util.GrowableByteBuffer.computeNewSize;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class GrowableByteBufferTest {
    private byte TEST_BYTE = 42;
    private char TEST_CHAR = '☃';
    private short TEST_SHORT = 1337;
    private int TEST_INT = 2752470;
    private long TEST_LONG = 180388626432L;
    private float TEST_FLOAT = (float)Math.PI;
    private double TEST_DOUBLE = Math.E;
    private byte[] TEST_ARRAY = {10, 100, 12, 120, 15, -23};
    private int SUB_ARRAY_OFF = 2;
    private int SUB_ARRAY_LEN = 2;
    private byte[] TEST_SUB_ARRAY = Arrays.copyOfRange(TEST_ARRAY, SUB_ARRAY_OFF, SUB_ARRAY_OFF + SUB_ARRAY_LEN);

    
    @Test
    public void miscBufferMethods() {
        final int SIZE = 10;
        final GrowableByteBuffer gbb = gbb(SIZE);
        assertEquals("Capacity is starting size", SIZE, gbb.capacity());
        assertEquals("Initial position", 0, gbb.position());
        assertEquals("Initial limit is starting size", SIZE, gbb.limit());

        gbb.position(4);
        assertEquals("Set position", 4, gbb.position());

        gbb.mark();
        gbb.position(6);
        gbb.reset();
        assertEquals("Rest returns to mark", 4, gbb.position());

        gbb.limit(5);
        assertEquals("Set limit", 5, gbb.limit());
        
        gbb.flip();
        assertEquals("Flip resets position", 0, gbb.position());
        assertEquals("Flip sets limit", 4, gbb.limit());

        gbb.position(2);
        gbb.rewind();
        assertEquals("Rewind resets position", 0, gbb.position());
        assertEquals("Rewind leaves limit", 4, gbb.limit());
        
        gbb.position(2);
        assertEquals("Remaining is computed", 2, gbb.remaining());
        assertEquals("Has remaining", true, gbb.hasRemaining());

        gbb.position(4);
        assertEquals("Has remaining", false, gbb.hasRemaining());
        
        gbb.clear();
        assertEquals("Clear resets position", 0, gbb.position());
        assertEquals("Clear resets limit", SIZE, gbb.limit());
    }

    @Test
    public void backedByHeapBuffer() {
        final GrowableByteBuffer gbb = gbb(10);
        assertEquals("Internal is not direct", true, !gbb.getInternalBuffer().isDirect());
        assertEquals("Not read only", false, gbb.isReadOnly());
        assertEquals("Has an array", true, gbb.hasArray());
        assertNotNull("Array is valid", gbb.array());
        assertEquals("No array offset", 0, gbb.arrayOffset());
    }
    
    @Test
    public void byteOrdering() {
        final GrowableByteBuffer gbb = gbb(10);
        assertEquals("Default byte order", ByteOrder.BIG_ENDIAN, gbb.order());
        gbb.order(ByteOrder.LITTLE_ENDIAN);
        assertEquals("Set byte order", ByteOrder.LITTLE_ENDIAN, gbb.order());
    }

    @Test
    public void getSizes() {
        final int INITIAL = 10;
        final int MAX_CACHE = 1234;
        final int MAX_BURST = 5678;
        final GrowableByteBuffer gbb = new GrowableByteBuffer(INITIAL, MAX_CACHE, MAX_BURST);
        assertEquals("initialSize", INITIAL, gbb.getInitialSize());
        assertEquals("maxCacheSize", MAX_CACHE, gbb.getMaxCacheSize());
        assertEquals("maxBurstSize", MAX_BURST, gbb.getMaxBurstSize());
    }
    
    @Test
    public void relativePutAndGet() {
        final GrowableByteBuffer gbb = gbb(4096);
        gbb.clear(); assertEquals("byte test", TEST_BYTE, putFlipGet(gbb, TEST_BYTE));
        gbb.clear(); assertEquals("char test", TEST_CHAR, putFlipGet(gbb, TEST_CHAR));
        gbb.clear(); assertEquals("short test", TEST_SHORT, putFlipGet(gbb, TEST_SHORT));
        gbb.clear(); assertEquals("int test", TEST_INT, putFlipGet(gbb, TEST_INT));
        gbb.clear(); assertEquals("long test", TEST_LONG, putFlipGet(gbb, TEST_LONG));
        gbb.clear(); assertEquals("float test", TEST_FLOAT, putFlipGet(gbb, TEST_FLOAT), 0);
        gbb.clear(); assertEquals("double test", TEST_DOUBLE, putFlipGet(gbb, TEST_DOUBLE), 0);
        gbb.clear(); assertArrayEquals("array test", TEST_ARRAY, putFlipGet(gbb, TEST_ARRAY));
        gbb.clear(); assertArrayEquals("sub-array test", TEST_SUB_ARRAY,
                                       putFlipGet(gbb, TEST_ARRAY, SUB_ARRAY_OFF, SUB_ARRAY_LEN));
    }

    @Test
    public void exactSizedRelativePutAndGet() {
        assertEquals("byte test", TEST_BYTE, putFlipGet(gbb(1), TEST_BYTE));
        assertEquals("char test", TEST_CHAR, putFlipGet(gbb(2), TEST_CHAR));
        assertEquals("short test", TEST_SHORT, putFlipGet(gbb(2), TEST_SHORT));
        assertEquals("int test", TEST_INT, putFlipGet(gbb(4), TEST_INT));
        assertEquals("long test", TEST_LONG, putFlipGet(gbb(8), TEST_LONG));
        assertEquals("float test", TEST_FLOAT, putFlipGet(gbb(4), TEST_FLOAT), 0);
        assertEquals("double test", TEST_DOUBLE, putFlipGet(gbb(8), TEST_DOUBLE), 0);
        assertArrayEquals("array test", TEST_ARRAY, putFlipGet(gbb(TEST_ARRAY.length), TEST_ARRAY));
        assertArrayEquals("sub-array test", TEST_SUB_ARRAY, putFlipGet(gbb(SUB_ARRAY_LEN), TEST_ARRAY, SUB_ARRAY_OFF, SUB_ARRAY_LEN));
    }

    @Test
    public void absolutePutAndGet() {
        final int OFFSET = 37;
        final GrowableByteBuffer gbb = gbb(100);
        assertEquals("byte test", TEST_BYTE, putGet(gbb, OFFSET, TEST_BYTE));
        assertEquals("char test", TEST_CHAR, putGet(gbb, OFFSET, TEST_CHAR));
        assertEquals("short test", TEST_SHORT, putGet(gbb, OFFSET, TEST_SHORT));
        assertEquals("int test", TEST_INT, putGet(gbb, OFFSET, TEST_INT));
        assertEquals("long test", TEST_LONG, putGet(gbb, OFFSET, TEST_LONG));
        assertEquals("float test", TEST_FLOAT, putGet(gbb, OFFSET, TEST_FLOAT), 0);
        assertEquals("double test", TEST_DOUBLE, putGet(gbb, OFFSET, TEST_DOUBLE), 0);
    }

    @Test
    public void exactSizedAbsolutePutAndGet() {
        final int OFFSET = 1;
        assertEquals("byte test", TEST_BYTE, putGet(gbb(1 + 1), OFFSET, TEST_BYTE));
        assertEquals("char test", TEST_CHAR, putGet(gbb(2 + 1), OFFSET, TEST_CHAR));
        assertEquals("short test", TEST_SHORT, putGet(gbb(2 + 1), OFFSET, TEST_SHORT));
        assertEquals("int test", TEST_INT, putGet(gbb(4 + 1), OFFSET, TEST_INT));
        assertEquals("long test", TEST_LONG, putGet(gbb(8 + 1), OFFSET, TEST_LONG));
        assertEquals("float test", TEST_FLOAT, putGet(gbb(4 + 1), OFFSET, TEST_FLOAT), 0);
        assertEquals("double test", TEST_DOUBLE, putGet(gbb(8 + 1), OFFSET, TEST_DOUBLE), 0);
    }
    
    @Test
    public void startWithZeroSizeAndGrow() {
        final GrowableByteBuffer gbb = new GrowableByteBuffer(0, 10);
        final byte[] bytes = { 5, 12, -10, 17, 50};
        putByteIncrementally(gbb, bytes.length);
        gbb.flip();
        getByteIncrementally(gbb, bytes.length);
    }

    @Test
    public void canGrowFromEachPut() {
        final int MAX = 100;
        assertEquals("byte test", TEST_BYTE, putFlipGet(gbb(0, MAX), TEST_BYTE));
        assertEquals("char test", TEST_CHAR, putFlipGet(gbb(1, MAX), TEST_CHAR));
        assertEquals("short test", TEST_SHORT, putFlipGet(gbb(1, MAX), TEST_SHORT));
        assertEquals("int test", TEST_INT, putFlipGet(gbb(3, MAX), TEST_INT));
        assertEquals("long test", TEST_LONG, putFlipGet(gbb(7, MAX), TEST_LONG));
        assertEquals("float test", TEST_FLOAT, putFlipGet(gbb(3, MAX), TEST_FLOAT), 0);
        assertEquals("double test", TEST_DOUBLE, putFlipGet(gbb(7, MAX), TEST_DOUBLE), 0);
        assertArrayEquals("array test", TEST_ARRAY, putFlipGet(gbb(TEST_ARRAY.length - 1, MAX), TEST_ARRAY));
        assertArrayEquals("sub-array test", TEST_SUB_ARRAY, putFlipGet(gbb(SUB_ARRAY_LEN - 1, MAX), TEST_ARRAY, SUB_ARRAY_OFF, SUB_ARRAY_LEN));
    }

    @Test
    public void growUpToMaxCacheSize() {
        final int START_SIZE = 10;
        final int MAX_SIZE = 100;
        final GrowableByteBuffer gbb = new GrowableByteBuffer(START_SIZE, MAX_SIZE);

        putByteIncrementally(gbb, MAX_SIZE);
        assertEquals("Max capacity is cache size", MAX_SIZE, gbb.capacity());
        cannotPutByte(gbb);
        gbb.flip();
        getByteIncrementally(gbb, MAX_SIZE);
    }

    @Test
    public void growUpToBurstSize() {
        final int START_SIZE = 5;
        final int CACHE_SIZE = 100;
        final int BURST_SIZE = 200;
        final GrowableByteBuffer gbb = new GrowableByteBuffer(START_SIZE, CACHE_SIZE, BURST_SIZE);

        putByteIncrementally(gbb, BURST_SIZE);
        assertEquals("Max capacity is burst size", BURST_SIZE, gbb.capacity());
        cannotPutByte(gbb);

        gbb.flip();
        getByteIncrementally(gbb, BURST_SIZE);
        
        gbb.clear();
        assertEquals("Clear reuses buffer with max cache sized", CACHE_SIZE, gbb.capacity());
    }

    @Test
    public void growSizeComputation() {
        assertEquals("grow from 0 is capped to maxCache", 5, computeNewSize(0, 1, 5, 6));
        assertEquals("grow from non-0 is capped to maxCache", 5, computeNewSize(4, 5, 5, 6));
        assertEquals("grow from maxCache is capped to maxBurst", 6, computeNewSize(5, 6, 5, 6));
        assertEquals("grow from maxBurst is capped to maxBurst", 6, computeNewSize(6, 7, 5, 6));
    }
    
    @Test
    public void wrappedArray() {
        final byte[] array = { 10, 15, 42, -10, 6, -5, 127 };
        final GrowableByteBuffer gbb = GrowableByteBuffer.wrap(array);
        assertEquals("initialSize", array.length, gbb.getInitialSize());
        assertEquals("maxCacheSize", array.length, gbb.getMaxCacheSize());
        assertEquals("maxBurstSize", array.length, gbb.getMaxBurstSize());
        
        for(int i = 0; i < array.length; ++i) {
            assertEquals("byte matches backing array, index " + i, array[i], gbb.get());
        }
        cannotPutByte(gbb);
        
        gbb.clear();
        putByteIncrementally(gbb, array.length);
        gbb.flip();
        for(int i = 0; i < array.length; ++i) {
            assertEquals("put went to backing array, index i", (byte)i, gbb.get());
        }
    }
    
    @Test
    public void prepareForSize() {
        final GrowableByteBuffer gbb = new GrowableByteBuffer(50, 100, 200);
        assertEquals("Can prepare size < current < cacheMax", true, gbb.prepareForSize(25));
        assertEquals("Did not shrink buffer", 50, gbb.capacity());
        
        assertEquals("Can prepare current < size < cacheMax", true, gbb.prepareForSize(75));
        assertEquals("Grew to exact size", 75, gbb.capacity());
        assertEquals("Did not cache previous", true, gbb.getCached() == null);
        
        assertEquals("Can prepare cacheMax < size < burstMax", true, gbb.prepareForSize(150));
        assertEquals("Grew to exact size", 150, gbb.capacity());
        assertEquals("Did cache previous", true, gbb.getCached() != null);
        assertEquals("Cached previous size", 75, gbb.getCached().capacity());

        assertEquals("Cannot prepare size > burstMax", false, gbb.prepareForSize(250));

        assertEquals("Currently have cached", true, gbb.getCached() != null);
        assertEquals("Can prepare size < cacheMax < current", true, gbb.prepareForSize(70));
        assertEquals("Used cached", true, gbb.getCached() == null);
        assertEquals("Current capacity", 75, gbb.capacity());
    }

    @Test
    public void growWithUserDefinedLimitIsPreserved() {
        final int SIZE = 10;
        final int MAX = 11;
        final int LIMIT = 8;
        final GrowableByteBuffer gbb = gbb(SIZE, MAX);
        gbb.limit(LIMIT);
        putByteIncrementally(gbb, LIMIT);

        assertEquals("space remaining", 0, gbb.remaining());
        assertEquals("capacity - limit", SIZE - LIMIT, gbb.capacity() - gbb.limit());

        gbb.put((byte)0);
        assertEquals("limit after growth", MAX - (SIZE - LIMIT), gbb.limit());
        assertEquals("capacity after growth", MAX, gbb.capacity());
        assertEquals("space remaining after put", 0, gbb.remaining());
        assertEquals("capacity - limit after put", MAX - LIMIT - 1, gbb.capacity() - gbb.limit());
    }

    @Test(expected=IllegalArgumentException.class)
    public void negativeInitialSize() {
        new GrowableByteBuffer(-1, 10, 15);
    }

    @Test(expected=IllegalArgumentException.class)
    public void initialGreaterThanCache() {
        new GrowableByteBuffer(15, 10, 20);
    }

    @Test(expected=IllegalArgumentException.class)
    public void cacheGreaterThanBurst() {
        new GrowableByteBuffer(10, 20, 15);
    }

    @Test(expected=BufferOverflowException.class)
    public void cannotGrowFromByte() {
        gbbWithRemaining(0).put(TEST_BYTE);
    }

    @Test(expected=BufferOverflowException.class)
    public void cannotGrowFromChar() {
        gbbWithRemaining(1).putChar(TEST_CHAR);
    }

    @Test(expected=BufferOverflowException.class)
    public void cannotGrowFromShort() {
        gbbWithRemaining(1).putShort(TEST_SHORT);
    }

    @Test(expected=BufferOverflowException.class)
    public void cannotGrowFromInt() {
        gbbWithRemaining(3).putInt(TEST_INT);
    }

    @Test(expected=BufferOverflowException.class)
    public void cannotGrowFromLong() {
        gbbWithRemaining(7).putLong(TEST_LONG);
    }

    @Test(expected=BufferOverflowException.class)
    public void cannotGrowFromFloat() {
        gbbWithRemaining(3).putFloat(TEST_FLOAT);
    }

    @Test(expected=BufferOverflowException.class)
    public void cannotGrowFromDouble() {
        gbbWithRemaining(7).putDouble(TEST_DOUBLE);
    }
    
    @Test(expected=BufferOverflowException.class)
    public void cannotGrowFromArray() {
        gbbWithRemaining(TEST_ARRAY.length - 1).put(TEST_ARRAY);
    }
    @Test(expected=BufferOverflowException.class)
    public void cannotGrowFromSubArray() {
        gbbWithRemaining(SUB_ARRAY_LEN - 1).put(TEST_ARRAY, SUB_ARRAY_OFF, SUB_ARRAY_LEN);
    }


    private static GrowableByteBuffer gbb(int sizeAndMax) {
        return new GrowableByteBuffer(sizeAndMax, sizeAndMax);
    }

    private static GrowableByteBuffer gbb(int size, int max) {
        return new GrowableByteBuffer(size, max);
    }

    private GrowableByteBuffer gbbWithRemaining(int remaining) {
        final int SIZE = 100;
        GrowableByteBuffer gbb = gbb(SIZE);
        gbb.position(SIZE - remaining);
        assertEquals("Space left", remaining, gbb.remaining());
        return gbb;
    }
    
    private static void putByteIncrementally(GrowableByteBuffer gbb, int count) {
        for(int i = 0; i < count; ++i) {
            gbb.put((byte)i);
        }
    }
    
    private static void getByteIncrementally(GrowableByteBuffer gbb, int count) {
        for(int i = 0 ; i < count; ++i) {
            assertEquals("Byte at index " + i, (byte)i, gbb.get());
        }
    }
    
    private static void cannotPutByte(GrowableByteBuffer gbb) {
        try {
            gbb.put((byte)-1);
            fail("Expected is full exception!: " + gbb);
        } catch(BufferOverflowException e) {
            // Expected
        }
    }

    private static byte putFlipGet(GrowableByteBuffer g, byte v)     { g.put(v);       g.flip(); return g.get(); }
    private static char putFlipGet(GrowableByteBuffer g, char v)     { g.putChar(v);   g.flip(); return g.getChar(); }
    private static short putFlipGet(GrowableByteBuffer g, short v)   { g.putShort(v);  g.flip(); return g.getShort(); }
    private static int putFlipGet(GrowableByteBuffer g, int v)       { g.putInt(v);    g.flip(); return g.getInt(); }
    private static long putFlipGet(GrowableByteBuffer g, long v)     { g.putLong(v);   g.flip(); return g.getLong(); }
    private static float putFlipGet(GrowableByteBuffer g, float v)   { g.putFloat(v);  g.flip(); return g.getFloat(); }
    private static double putFlipGet(GrowableByteBuffer g, double v) { g.putDouble(v); g.flip(); return g.getDouble(); }

    private static byte putGet(GrowableByteBuffer g, int i, byte v)     { g.put(i, v);       return g.get(i); }
    private static char putGet(GrowableByteBuffer g, int i, char v)     { g.putChar(i, v);   return g.getChar(i); }
    private static short putGet(GrowableByteBuffer g, int i, short v)   { g.putShort(i, v);  return g.getShort(i); }
    private static int putGet(GrowableByteBuffer g, int i, int v)       { g.putInt(i, v);    return g.getInt(i); }
    private static long putGet(GrowableByteBuffer g, int i, long v)     { g.putLong(i, v);   return g.getLong(i); }
    private static float putGet(GrowableByteBuffer g, int i, float v)   { g.putFloat(i, v);  return g.getFloat(i); }
    private static double putGet(GrowableByteBuffer g, int i, double v) { g.putDouble(i, v); return g.getDouble(i); }
    
    private static byte[] putFlipGet(GrowableByteBuffer g, byte[] v) {
        byte[] temp = new byte[v.length];
        g.put(v); g.flip(); g.get(temp); return temp;
    }

    private static byte[] putFlipGet(GrowableByteBuffer g, byte[] v, int off, int len) {
        byte[] temp = new byte[len];
        g.put(v, off, len); g.flip(); g.get(temp); return temp;
    }
}
