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

public final class WrappingByteSource implements ByteSource {
    // WrappingByteSource interface

    public WrappingByteSource wrap(byte[] bytes, int offset, int length) {
        ArgumentValidation.notNull("byte array", bytes);
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

    // private methods

    // object state

    private byte[] bytes;
    private int offset;
    private int length;
}
