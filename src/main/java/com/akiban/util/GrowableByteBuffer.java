/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

package com.akiban.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.InvalidMarkException;

public class GrowableByteBuffer implements Comparable<GrowableByteBuffer> {
    public static final class GrowableByteBufferIsFullException extends RuntimeException {
    }

    private static final int BYTE_LEN   = 1;
    private static final int CHAR_LEN   = 2;
    private static final int SHORT_LEN  = 2;
    private static final int INT_LEN    = 4;
    private static final int LONG_LEN   = 8;
    private static final int FLOAT_LEN  = 4;
    private static final int DOUBLE_LEN = 8;
    private static final int GROW_SIZE_FROM_ZERO = 4096;


    private final int initialSize;
    private final int maxCacheSize;
    private final int maxBurstSize;
    private ByteBuffer buffer;
    private ByteBuffer cached;


    public GrowableByteBuffer(int initialSizeAndMax) {
        this(initialSizeAndMax, initialSizeAndMax, initialSizeAndMax);
    }

    public GrowableByteBuffer(int initialSize, int maxSize) {
        this(initialSize, maxSize, maxSize);
    }

    public GrowableByteBuffer(int initialSize, int maxCacheSize, int maxBurstSize) {
        ArgumentValidation.isNotNegative("initialSize", initialSize);
        ArgumentValidation.isTrue("maxCacheSize >= initialSize", maxCacheSize >= initialSize);
        ArgumentValidation.isTrue("maxBurstSize >= maxCacheSize", maxBurstSize >= maxCacheSize);
        this.initialSize = initialSize;
        this.maxCacheSize = maxCacheSize;
        this.maxBurstSize = maxBurstSize;
        buffer = ByteBuffer.allocate(initialSize);
    }

    private GrowableByteBuffer(ByteBuffer buffer) {
        final int capacity = buffer.capacity();
        this.initialSize = capacity;
        this.maxCacheSize = capacity;
        this.maxBurstSize = capacity;
        this.buffer = buffer;
    }

    public static GrowableByteBuffer wrap(byte[] array) {
        return new GrowableByteBuffer(ByteBuffer.wrap(array));
    }
    
    public int getInitialSize() {
        return initialSize;
    }
    
    public int getMaxCacheSize() {
        return maxCacheSize;
    }
    
    public int getMaxBurstSize() {
        return maxBurstSize;
    }

    public ByteBuffer getInternalBuffer() {
        return buffer;
    }


    @Override
    public String toString() {
        return "[init= " + initialSize +
                " maxCache=" + maxCacheSize +
                " maxBurst=" + maxBurstSize +
                " " + buffer.toString() + "]";
    }

    @Override
    public int hashCode() {
        return buffer.hashCode();
    }

    @Override
    public boolean equals(Object rhs) {
        if(this == rhs) {
            return true;
        }
        if(!(rhs instanceof GrowableByteBuffer)) {
            return false;
        }
        return buffer.equals(((GrowableByteBuffer)rhs).buffer);
    }

    @Override
    public int compareTo(GrowableByteBuffer that) {
        return buffer.compareTo(that.buffer);
    }


    //
    // Buffer - trivial wrappers
    //
    
    public int capacity() {
        return buffer.capacity();
    }

    public int position() {
        return buffer.position();
    }

    public GrowableByteBuffer position(int position) {
        buffer.position(position);
        return this;
    }

    public int limit() {
        return buffer.limit();
    }

    public GrowableByteBuffer limit(int limit) {
        buffer.limit(limit);
        return this;
    }

    public GrowableByteBuffer mark() {
        buffer.mark();
        return this;
    }

    public GrowableByteBuffer reset() {
        buffer.reset();
        return this;
    }

    public GrowableByteBuffer clear() {
        if(cached != null) {
            buffer = cached;
            cached = null;
        }
        buffer.clear();
        return this;
    }

    public GrowableByteBuffer flip() {
        buffer.flip();
        return this;
    }

    public GrowableByteBuffer rewind() {
        buffer.rewind();
        return this;
    }

    public int remaining() {
        return buffer.remaining();
    }

    public boolean hasRemaining() {
        return buffer.hasRemaining();
    }

    public boolean isReadOnly() {
        return buffer.isReadOnly();
    }

    public boolean hasArray() {
        return buffer.hasArray();
    }

    public byte[] array() {
        return buffer.array();
    }

    public int arrayOffset() {
        return buffer.arrayOffset();
    }


    //
    // ByteBuffer, misc - trivial wrapper
    //

    public ByteOrder order() {
        return buffer.order();
    }

    public GrowableByteBuffer order(ByteOrder byteOrder) {
        buffer.order(byteOrder);
        return this;
    }

    public GrowableByteBuffer compact() {
        buffer.compact();
        return this;
    }


    //
    // ByteBuffer, gets - trivial wrappers
    //

    public byte get() {
        return buffer.get();
    }

    public char getChar() {
        return buffer.getChar();
    }

    public short getShort() {
        return buffer.getShort();
    }

    public int getInt() {
        return buffer.getInt();
    }

    public long getLong() {
        return buffer.getLong();
    }

    public float getFloat() {
        return buffer.getFloat();
    }

    public double getDouble() {
        return buffer.getDouble();
    }

    public byte get(int index) {
        return buffer.get(index);
    }

    public char getChar(int index) {
        return buffer.getChar(index);
    }

    public short getShort(int index) {
        return buffer.getShort(index);
    }

    public int getInt(int index) {
        return buffer.getInt(index);
    }

    public long getLong(int index) {
        return buffer.getLong(index);
    }

    public float getFloat(int index) {
        return buffer.getFloat(index);
    }

    public double getDouble(int index) {
        return buffer.getDouble(index);
    }

    public GrowableByteBuffer get(byte[] dest) {
        buffer.get(dest);
        return this;
    }

    public GrowableByteBuffer get(byte[] dest, int destOffset, int length) {
        buffer.get(dest, destOffset, length);
        return this;
    }


    //
    // ByteBuffer, puts - checked wrappers
    //

    public GrowableByteBuffer put(byte value) {
        checkPut(BYTE_LEN);
        buffer.put(value);
        return this;
    }

    public GrowableByteBuffer putChar(char value) {
        checkPut(CHAR_LEN);
        buffer.putChar(value);
        return this;
    }

    public GrowableByteBuffer putShort(short value) {
        checkPut(SHORT_LEN);
        buffer.putShort(value);
        return this;
    }

    public GrowableByteBuffer putInt(int value) {
        checkPut(INT_LEN);
        buffer.putInt(value);
        return this;
    }

    public GrowableByteBuffer putLong(long value) {
        checkPut(LONG_LEN);
        buffer.putLong(value);
        return this;
    }

    public GrowableByteBuffer putFloat(float value) {
        checkPut(FLOAT_LEN);
        buffer.putFloat(value);
        return this;
    }

    public GrowableByteBuffer putDouble(double value) {
        checkPut(DOUBLE_LEN);
        buffer.putDouble(value);
        return this;
    }

    public GrowableByteBuffer put(int index, byte value) {
        checkPut(index, BYTE_LEN);
        buffer.put(index, value);
        return this;
    }

    public GrowableByteBuffer putChar(int index, char value) {
        checkPut(index, CHAR_LEN);
        buffer.putChar(index, value);
        return this;
    }

    public GrowableByteBuffer putShort(int index, short value) {
        checkPut(index, SHORT_LEN);
        buffer.putShort(index, value);
        return this;
    }

    public GrowableByteBuffer putInt(int index, int value) {
        checkPut(index, INT_LEN);
        buffer.putInt(index, value);
        return this;
    }

    public GrowableByteBuffer putLong(int index, long value) {
        checkPut(index, LONG_LEN);
        buffer.putLong(index, value);
        return this;
    }

    public GrowableByteBuffer putFloat(int index, float value) {
        checkPut(index, FLOAT_LEN);
        buffer.putFloat(index, value);
        return this;
    }

    public GrowableByteBuffer putDouble(int index, double value) {
        checkPut(index, DOUBLE_LEN);
        buffer.putDouble(index, value);
        return this;
    }

    public GrowableByteBuffer put(ByteBuffer source) {
        checkPut(source.remaining());
        buffer.put(source);
        return this;
    }

    public GrowableByteBuffer put(byte[] source) {
        checkPut(source.length);
        buffer.put(source);
        return this;
    }

    public GrowableByteBuffer put(byte[] source, int sourceOffset, int length) {
        checkPut(length);
        buffer.put(source, sourceOffset, length);
        return this;
    }


    private void checkPut(int size) {
        checkPut(buffer.position(), size);
    }
    
    private void checkPut(int index, int length) {
        if(index < 0 || (buffer.position() + length <= buffer.capacity())) {
            return; // Invalid or fits, ByteBuffer methods will handle it
        }

        final int newSize = computeNewSize(buffer.capacity(), maxCacheSize, maxBurstSize);
        if(newSize == buffer.capacity()) {
            throw new GrowableByteBufferIsFullException();
        }

        grow(newSize);
    }

    private void grow(int newSize) {
        int saveMark = -1;
        try {
            int savePos = buffer.position();
            buffer.reset();
            saveMark = buffer.position();
            buffer.position(savePos);
        } catch(InvalidMarkException e) {
            // Was no mark to save
        }

        ByteBuffer old = buffer;
        buffer = ByteBuffer.allocate(newSize);
        old.flip();
        buffer.put(old);
        buffer.order(old.order());

        if(saveMark != -1) {
            int curPos = buffer.position();
            buffer.position(saveMark);
            buffer.mark();
            buffer.position(curPos);
        }

        if(newSize > maxCacheSize && old.capacity() <= maxCacheSize) {
            cached = old;
        }
    }

    static int computeNewSize(int curSize, int maxCache, int maxBurst) {
        assert maxCache <= maxBurst : curSize + ", " + maxCache + " <= " + maxBurst;
        final int doubled = curSize * 2;
        if(curSize == 0) {
            return Math.min(GROW_SIZE_FROM_ZERO, maxCache);
        }
        if(curSize < maxCache) {
            return Math.min(doubled,  maxCache);
        }
        if(curSize < maxBurst) {
            return Math.min(doubled,  maxBurst);
        }
        return curSize;
    }
}
