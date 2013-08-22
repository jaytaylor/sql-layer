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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.InvalidMarkException;

/**
 * <p>
 * An API compatible, drop-in replacement for {@link ByteBuffer} that provides
 * dynamic growth on put operations. This is useful for situations where the
 * ByteBuffer interface is useful/required, but the final size of the buffer
 * is not known in advance.
 * </p>
 * <p>
 * Most methods delegate in a trivially to the underlying ByteBuffer, which is
 * always a buffer that {@link java.nio.ByteBuffer#hasArray()}, without any
 * intervention. The various put() methods are checked before passing onto the
 * underlying buffer to ensure adequate room is available. If there isn't, a 
 * new underlying buffer will be allocated, the existing state copied over, and
 * then the put is performed. If the growth bounds were to be exceeded for the
 * required size, no growth is performed and the standard
 * {@link java.nio.BufferOverflowException} will be thrown.
 * </p>
 * <p>
 * Certain methods cannot be trivially or transparently wrapped, such as 
 * {@link java.nio.ByteBuffer#asCharBuffer()}, so they are not exposed directly
 * through the GrowableByteBuffer interface. Instead, the underlying buffer
 * can be acquired through the {@link #getInternalBuffer()} and accessed as
 * needed. Note that only one interface, Growable or the internal, should be
 * modified at a time. Since any put on the Growable could cause the internal
 * buffer to be reallocated, <i>mixing puts will quickly lead towards working
 * on disparate buffers</i>.
 * </p>
 * <p>
 * There are three distinct sizes available for parametrization at construction
 * time. These are the initial size, maximum size to cache, and maximum size
 * to allow growth to (now refereed to as burst size). Initial is how big the
 * underlying buffer starts at (allowed to be <code>0</code>). Maximum cache
 * is how big the buffer can grow and still be retained for. Burst is how big
 * the buffer can grow, but will be discarded for the previous buffer on the
 * next reset ({@link #clear()} or {@link #prepareForSize(int)}).
 * </p>
 */
public class GrowableByteBuffer implements Comparable<GrowableByteBuffer> {
    private static final int BYTE_LEN   = 1;
    private static final int CHAR_LEN   = 2;
    private static final int SHORT_LEN  = 2;
    private static final int INT_LEN    = 4;
    private static final int LONG_LEN   = 8;
    private static final int FLOAT_LEN  = 4;
    private static final int DOUBLE_LEN = 8;
    private static final int GROW_SIZE_FROM_ZERO = 4096;

    private static enum PreserveLimit {
        NONE,
        ABSOLUTE,
        RELATIVE
    }

    private final int initialSize;
    private final int maxCacheSize;
    private final int maxBurstSize;
    private ByteBuffer buffer;
    private ByteBuffer cached;


    /**
     * New instance where the initial, maximum cache, and maximum burst
     * are all the same size. Note that this creates an un-growable buffer,
     * but is provided for convenience.
     *
     * @param initialSizeAndMax size for all parameters
     */
    public GrowableByteBuffer(int initialSizeAndMax) {
        this(initialSizeAndMax, initialSizeAndMax, initialSizeAndMax);
    }

    /**
     * New instance where the maximum cache and maximum burst are the same
     * size. This buffer will grow from the given initial size up to the max.
     *
     * @param initialSize starting size of the buffer
     * @param maxSize maximum size for cache and burst
     *
     * @throws IllegalArgumentException if either parameter is negative or
     * initialSize is less than maxSize
     */
    public GrowableByteBuffer(int initialSize, int maxSize) {
        this(initialSize, maxSize, maxSize);
    }

    /**
     * New instance where all parameters size parameters are specified
     * individually. The buffer will grow from initial up to the maximum
     * burst, but only hold onto a buffer <= maximum cache size across
     * {@link #clear()}s.
     *
     * @param initialSize starting size of the buffer
     * @param maxCacheSize maximum size buffer to hold onto
     * @param maxBurstSize maximum size the buffer can ever grow to
     *
     * @throws IllegalArgumentException if initial size is negative, maximum
     * cache is less than initial, or maximum burst is less than maximum cache.
     */
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

    /**
     * New instance that is backed by the given array. This is trivially
     * serviced by {@link ByteBuffer#wrap(byte[])} method. The maximum
     * cache and maximum burst size of this buffer will be
     * <code>array.length</code> so no growth is possible.
     *
     * @param array array to wrap
     *
     * @return New instance backed by array
     */
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

    public boolean prepareForSize(int size) {
        if(size <= buffer.capacity()) {
            if(cached != null && size <= cached.capacity()) {
                cached.clear();
                copyBufferState(PreserveLimit.ABSOLUTE, buffer, cached);
                useCached();
            }
            return true;
        }
        if(size <= maxBurstSize) {
            grow(PreserveLimit.ABSOLUTE, size);
            return true;
        }
        return false;
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
            useCached();
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
        if(index < 0 || (length <= buffer.remaining())) {
            return; // Invalid or fits, ByteBuffer methods will handle it
        }

        final int slack = buffer.capacity() - buffer.limit();
        final int required = buffer.position() + length + slack;
        final int newSize = computeNewSize(buffer.capacity(), required, maxCacheSize, maxBurstSize);
        if(newSize == buffer.capacity()) {
            return; // Can't grow anymore, BOE will be thrown by put
        }

        grow(PreserveLimit.RELATIVE, newSize);
    }

    private void grow(PreserveLimit preserve, int newSize) {
        ByteBuffer old = buffer;
        buffer = ByteBuffer.allocate(newSize);
        copyBufferState(preserve, old, buffer);
        if(newSize > maxCacheSize && old.capacity() <= maxCacheSize) {
            cached = old;
        }
    }

    private void useCached() {
        assert cached != null : this;
        buffer = cached;
        cached = null;
    }

    private static void copyBufferState(PreserveLimit preserve, ByteBuffer oldBB, ByteBuffer newBB) {
        int saveMark = -1;
        try {
            int savePos = oldBB.position();
            oldBB.reset();
            saveMark = oldBB.position();
            oldBB.position(savePos);
        } catch(InvalidMarkException e) {
            // Was no mark to save
        }

        final int oldLimit = oldBB.limit();
        oldBB.flip();
        newBB.put(oldBB);
        newBB.order(oldBB.order());

        if(preserve == PreserveLimit.ABSOLUTE) {
            newBB.limit(oldLimit);
        } else if(preserve == PreserveLimit.RELATIVE) {
            final int slack = oldBB.capacity() - oldLimit;
            newBB.limit(newBB.capacity() - slack);
        }

        if(saveMark != -1) {
            int curPos = newBB.position();
            newBB.position(saveMark);
            newBB.mark();
            newBB.position(curPos);
        }
    }


    //
    // Package private, for test
    //

    ByteBuffer getCached() {
        return cached;
    }

    static int computeNewSize(int curSize, int required, int maxCache, int maxBurst) {
        assert maxCache <= maxBurst : curSize + ", " + maxCache + " <= " + maxBurst;
        final int doubled = curSize * 2;
        if(curSize == 0 && required <= GROW_SIZE_FROM_ZERO) {
            return Math.min(GROW_SIZE_FROM_ZERO, maxCache);
        }
        if(curSize < maxCache && required <= maxCache) {
            return Math.min(Math.max(doubled, required),  maxCache);
        }
        if(curSize < maxBurst && required <= maxBurst) {
            return Math.min(Math.max(doubled, required),  maxBurst);
        }
        return curSize;
    }
}
