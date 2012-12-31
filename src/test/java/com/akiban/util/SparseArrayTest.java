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

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public final class SparseArrayTest {

    @Test
    public void getCreates() {
        CountingSparseArray tester = new CountingSparseArray();

        assertEquals("isDefined", false, tester.isDefined(0));
        assertEquals("first get", "1", tester.get(0));
        assertEquals("isDefined", true, tester.isDefined(0));

        assertEquals("isDefined", false, tester.isDefined(5));
        assertEquals("second get", "2", tester.get(5));
        assertEquals("isDefined", true, tester.isDefined(5));

        assertEquals("create count", 2, tester.count);
    }
    
    @Test
    public void getGrowsCapacity() {
        CountingSparseArray tester = new CountingSparseArray();
        int oldCapacity = tester.currentCapacity();

        int index = oldCapacity + 10;
        assertEquals("isDefined", false, tester.isDefined(index));
        assertEquals("first get", "1", tester.get(index));
        assertEquals("isDefined", true, tester.isDefined(index));

        int newCapacity = tester.currentCapacity();
        assertTrue("capacity <= index: " + newCapacity+ " <= " + index, newCapacity > index);

        assertEquals("create count", 1, tester.count);
    }

    @Test
    public void getReuses() {
        CountingSparseArray tester = new CountingSparseArray();

        assertEquals("isDefined", false, tester.isDefined(0));
        assertEquals("first get", "1", tester.get(0));
        assertEquals("isDefined", true, tester.isDefined(0));
        assertEquals("first get", "1", tester.get(0));
        assertEquals("isDefined", true, tester.isDefined(0));
        assertEquals("create count", 1, tester.count);
    }
    
    @Test
    public void setIndex() {
        CountingSparseArray tester = new CountingSparseArray();

        assertEquals("isDefined", false, tester.isDefined(0));
        tester.set(0, "foo");
        assertEquals("create count", 0, tester.count);
        assertEquals("get", "foo", tester.get(0));
        assertEquals("create count", 0, tester.count);
        assertEquals("isDefined", true, tester.isDefined(0));
        
        String old = tester.set(0, "bar");
        assertEquals("get", "bar", tester.get(0));
        assertEquals("get", "foo", old);
        assertEquals("isDefined", true, tester.isDefined(0));
        assertEquals("create count", 0, tester.count);
    }
    
    @Test
    public void setGrowsCapacity() {
        CountingSparseArray tester = new CountingSparseArray();
        int oldCapacity = tester.currentCapacity();

        int index = oldCapacity + 10;
        assertEquals("isDefined", false, tester.isDefined(index));
        String old = tester.set(index, "foo");
        assertEquals("old value", null, old);
        assertEquals("isDefined", true, tester.isDefined(index));

        int newCapacity = tester.currentCapacity();
        assertTrue("capacity <= index: " + newCapacity+ " <= " + index, newCapacity > index);

        assertEquals("create count", 0, tester.count);
    }

    @Test
    public void toListIsCompact() {
        SparseArray<Integer> array = new SparseArray<Integer>();
        array.set(0, 10);
        array.set(1, 11);
        List<Integer> list = array.toList();
        // Internal size may be bigger, but only get defined elements
        assertEquals("list size", 2, list.size());
        assertEquals("list get(0)", Integer.valueOf(10), list.get(0));
        assertEquals("list get(1)", Integer.valueOf(11), list.get(1));
    }

    @Test(expected=IllegalArgumentException.class)
    public void toListMustBeCompact() {
        SparseArray<Integer> array = new SparseArray<Integer>();
        array.set(5, 50);
        array.toList();
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void negativeIndex() {
        new CountingSparseArray().get(-1);
    }

    private static class CountingSparseArray extends SparseArray<String> {
        @Override
        protected String initialValue() {
            return Integer.toString(++count);
        }

        int count = 0;
    }
}
