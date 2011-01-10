package com.akiban.cserver.api.common;

import java.nio.ByteBuffer;

import com.akiban.util.ArgumentValidation;
import com.akiban.util.CacheMap;

public final class ColumnId extends ByteBufferWriter implements Comparable<ColumnId> {

    private final static CacheMap<Integer,ColumnId> cache = new CacheMap<Integer, ColumnId>(new CacheMap.Allocator<Integer,ColumnId>() {
        @Override
        public ColumnId allocateFor(Integer key) {
            return new ColumnId(key);
        }
    });

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
