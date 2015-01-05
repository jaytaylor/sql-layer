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
import java.util.Arrays;

public final class WrappingByteSource implements ByteSource {

    // WrappingByteSource interface

    /**
     * Converts an array-backed ByteBuffer to a WrappingByteSource.
     * @param byteBuffer the ByteBuffer that is wrapping a byte[]
     * @return a WrappingByteSource that represents the same byte[] wrapping as the incoming ByteBuffer
     * @throws NullPointerException if byteBuffer is null
     * @throws IllegalArgumentException if {@code byteBuffer.hasArray() == false}
     */
    public static WrappingByteSource fromByteBuffer(ByteBuffer byteBuffer) {
        if (!byteBuffer.hasArray()) {
            throw new IllegalArgumentException("incoming ByteBuffer must have a backing array");
        }
        return new WrappingByteSource().wrap(
                byteBuffer.array(),
                byteBuffer.arrayOffset() + byteBuffer.position(),
                byteBuffer.arrayOffset() + byteBuffer.limit() - byteBuffer.position()
        );
    }

    public WrappingByteSource wrap(byte[] bytes) {
        return wrap(bytes, 0, bytes.length);
    }

    public WrappingByteSource wrap(byte[] bytes, int offset, int length) {
        ArgumentValidation.notNull("byte array", bytes);
        if (bytes.length == 0) {
            ArgumentValidation.isEQ("length on wrapped byte[0]", length, 0);
            ArgumentValidation.isEQ("offset on wrapped byte[0]", offset, 0);
            this.bytes = bytes;
            this.offset = 0;
            this.length = 0;
            return this;
        }
        ArgumentValidation.isGTE("length", length, 0);
        boolean offsetError = offset < 0;
        if (length > 0)
            offsetError |= offset >= bytes.length;
        else
            offsetError |= offset > bytes.length;
        if (offsetError) {
            throw new IllegalArgumentException("offset must be between 0 and bytes.length (" + bytes.length + ')');
        }
        int lastIndex = offset + length;
        ArgumentValidation.isLTE("last index", lastIndex, bytes.length);

        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        return this;
    }

    public WrappingByteSource() {
        // nothing
    }

    public WrappingByteSource(byte[] bytes) {
        wrap(bytes);
    }

    public WrappingByteSource(byte[] bytes, int offset, int length) {
        wrap(bytes, offset, length);
    }

    // ByteSource interface

    @Override
    public byte[] byteArray() {
        return bytes;
    }

    @Override
    public int byteArrayOffset() {
        return offset;
    }

    @Override
    public int byteArrayLength() {
        return length;
    }

    // Comparable interface

    @Override
    public int compareTo(ByteSource o) {
        int minlength = Math.min(byteArrayLength(), o.byteArrayLength());
        byte[] obytes = o.byteArray();
        for (int i=0; i < minlength; ++i) {
            int myoffset = i + byteArrayOffset();
            int oofset = i + o.byteArrayOffset();
            int compare = bytes[myoffset] - obytes[oofset];
            if (compare != 0)
                return compare;
        }
        // all bytes were equal, return shorter array
        return byteArrayLength() - o.byteArrayLength();
    }

    @Override
    public byte[] toByteSubarray() {
        return Arrays.copyOfRange(bytes, offset,  offset+length);

    }

    // Object interface

    @Override
    public String toString() {
        return String.format("WrappingByteSource(byte[%d] offset=%d length=%d)", bytes.length, offset, length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WrappingByteSource that = (WrappingByteSource) o;

        if (length != that.length)
            return false;
        if (offset == that.offset && bytes == that.bytes)
            return true;
        for(int i=0; i < length; ++i) {
            if (bytes[offset+i] != that.bytes[that.offset+i])
                return false;
        }
        return true;

    }

    @Override
    public int hashCode() {
        // this isn't the greatest hash, but it should be good enough.
        // it relies on offset, length, and the first HASH_BYTES bytes (or as many bytes as are available).
        int result = 1;
        for (int i=0; i< length; ++i) {
            result = 31 * result + bytes[offset+i];
        }
        result = 31 * result + length;
        return result;
    }

    // object state

    private byte[] bytes;
    private int offset;
    private int length;

    // consts

    /**
     * The maximum number of bytes that will contribute to the object's hashCode
     */
    private final int HASH_BYTES = 5;

}
