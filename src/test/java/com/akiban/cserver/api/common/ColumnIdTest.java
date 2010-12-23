package com.akiban.cserver.api.common;

import static org.junit.Assert.assertEquals;

import java.util.TreeMap;

import org.junit.Test;

public final class ColumnIdTest {
    @Test
    public void testEquality() {
        ColumnId columnOne = ColumnId.of(1);
        ColumnId columnTwo = ColumnId.of(1);

        assertEquals("columns", columnOne, columnTwo);
        
        Object value = new Object();
        TreeMap<ColumnId,Object> mapOne = new TreeMap<ColumnId, Object>();
        mapOne.put(columnOne, value);
        TreeMap<ColumnId,Object> mapTwo = new TreeMap<ColumnId, Object>();
        mapTwo.put(columnOne, value);
        assertEquals("maps", mapOne, mapTwo);
    }
}
