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

package com.akiban.util;

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
     * @deprecated This method is intended to be used as a bridge while we convert the whole system to use
     * the new conversions system. Once that conversion is in place, we shouldn't need this method.
     */
    @Deprecated
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
        if (offset < 0 || offset >= bytes.length) {
            throw new IllegalArgumentException("offset must be between 0 and bytes.length (" + bytes.length + ')');
        }
        ArgumentValidation.isGTE("length", length, 0);
        int lastIndex = offset + length;
        ArgumentValidation.isLTE("last index", lastIndex, bytes.length);

        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        return this;
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

        return length == that.length && offset == that.offset && Arrays.equals(bytes, that.bytes);

    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(bytes);
        result = 31 * result + offset;
        result = 31 * result + length;
        return result;
    }

    // object state

    private byte[] bytes;
    private int offset;
    private int length;
}
