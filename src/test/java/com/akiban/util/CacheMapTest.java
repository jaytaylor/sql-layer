/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.util;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public final class CacheMapTest {

    @SuppressWarnings("unused")
    private static class HookedCacheMap<K,V> extends CacheMap<K,V>{
        private int allocations = 0;

        private HookedCacheMap() {
        }

        private HookedCacheMap(Allocator<K, V> kvAllocator) {
            super(kvAllocator);
        }

        private HookedCacheMap(int size) {
            super(size);
        }

        private HookedCacheMap(int size, Allocator<K, V> kvAllocator) {
            super(size, kvAllocator);
        }

        @Override
        protected void allocatorHook() {
            ++allocations;
        }
    }

    private static class TestAllocator implements CacheMap.Allocator<java.lang.Integer, java.lang.String> {
        @Override
        public String allocateFor(Integer key) {
            return new String(String.format("allocated key %d", key));
        }
    }

    @Test
    public void withAllocator() {
        HookedCacheMap<Integer,String> map = new HookedCacheMap<Integer, String>(new TestAllocator());

        final String result = map.get(1);
        assertEquals("map[1]", "allocated key 1", result);
        assertEquals("allocations", 1, map.allocations);
        assertSame("second call", result, map.get(1));
        assertEquals("allocations", 1, map.allocations);
        assertSame("removing", result, map.remove(1));

        final String result2 = map.get(1);
        assertEquals("map[1]", "allocated key 1", result2);
        if (result == result2) {
            fail("Expected new string");
        }
        assertEquals("allocations", 2, map.allocations);
    }

    @Test
    public void testEquality() {
        CacheMap<Integer,String> cacheMap = new CacheMap<Integer, String>();
        HashMap<Integer,String> expectedMap = new HashMap<Integer, String>();

        cacheMap.put(1, "one");
        expectedMap.put(1, "one");
        cacheMap.put(2, "two");
        expectedMap.put(2, "two");

        testEquality(true, expectedMap, cacheMap);
        testEquality(true, cacheMap, expectedMap);

        expectedMap.put(3, "three");
        testEquality(false, expectedMap, cacheMap);
        testEquality(false, cacheMap, expectedMap);
    }

    private static void testEquality(boolean shouldBeEqual, Map<?,?> map1, Map<?,?> map2) {
        if (shouldBeEqual != map1.equals(map2)) {
            fail(String.format("%s equals %s expected %s", map1, map2, shouldBeEqual));
        }
        int map1Hash = map1.hashCode();
        int map2Hash = map2.hashCode();
        if (shouldBeEqual != (map1Hash == map2Hash)) {
            fail(String.format("%d ?= %d expected %s", map1Hash, map2Hash, shouldBeEqual));
        }
    }

    @Test
    public void lru() {
        HookedCacheMap<Integer,String> map = new HookedCacheMap<Integer, String>(1, new TestAllocator());

        assertEquals("size", 0, map.size());

        assertNull("expected null", map.put(1, "explicit key a"));
        assertEquals("size", 1, map.size());
        
        assertEquals("old value", "explicit key a", map.put(1, "explicit key b"));
        assertEquals("size", 1, map.size());
        assertEquals("allocations", 0, map.allocations);

        assertEquals("generated value", "allocated key 2", map.get(2));
        assertEquals("size", 1, map.size());
        assertEquals("allocations", 1, map.allocations);

        Map<Integer,String> expectedMap = new HashMap<Integer, String>();
        expectedMap.put(2, "allocated key 2");
        assertEquals("size", 1, map.size());
        assertEquals("map values", expectedMap, map);
        assertEquals("allocations", 1, map.allocations);
    }

    @Test
    public void nullAllocator() {
        CacheMap<Integer,String> map = new CacheMap<Integer, String>(null);
        assertNull("expected null", map.get(1));
    }

    @Test
    public void nullKey() {
        CacheMap<Integer,String> map = new CacheMap<Integer, String>(null);
        map.put(null, "hello");
        assertEquals("get(null)", "hello", map.get(null));
    }

    @Test
    public void nullValue() {
        CacheMap<Integer,String> map = new CacheMap<Integer, String>(null);
        map.put(1, null);
        assertEquals("null value", null, map.get(1));
    }

    @Test(expected=IllegalArgumentException.class)
    public void sizeIsZero() {
        new CacheMap<Integer, String>(0);
    }

    @Test(expected=IllegalArgumentException.class)
    public void sizeIsNegative() {
        new CacheMap<Integer, String>(-10);
    }

    @Test(expected=ClassCastException.class)
    public void allocatorCastException() {
        @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
        CacheMap<Integer,String> map = new CacheMap<Integer, String>(new TestAllocator());
        map.get("1");
    }
}
