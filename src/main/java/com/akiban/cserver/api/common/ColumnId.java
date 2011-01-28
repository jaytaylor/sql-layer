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

package com.akiban.cserver.api.common;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import com.akiban.util.ArgumentValidation;
import com.akiban.util.CacheMap;

public final class ColumnId extends ByteBufferWriter implements Comparable<ColumnId> {

    private final static Map<Integer, ColumnId> cache = Collections.synchronizedMap(
            new CacheMap<Integer, ColumnId>(new CacheMap.Allocator<Integer, ColumnId>() {
                @Override
                public ColumnId allocateFor(Integer key) {
                    return new ColumnId(key);
                }
            })
    );

    private final int columnPosition;

    private ColumnId(int columnPosition) {
        ArgumentValidation.isNotNegative("position", columnPosition);
        this.columnPosition = columnPosition;
    }

    public static ColumnId of(int columnPosition) {
        return cache.get(columnPosition);
    }

    @Override
    protected void writeToBuffer(ByteBuffer output) throws Exception {
        output.putInt(columnPosition);
    }

    public int getPosition() {
        return columnPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnId columnId1 = (ColumnId) o;

        return columnPosition == columnId1.columnPosition;

    }

    @Override
    public int compareTo(ColumnId o) {
        return this.columnPosition - o.columnPosition;
    }

    @Override
    public int hashCode() {
        return columnPosition;
    }

    @Override
    public String toString() {
        return String.format("ColumnId<%d>", columnPosition);
    }
}
