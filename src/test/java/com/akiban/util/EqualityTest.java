
package com.akiban.util;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public final class EqualityTest {
    @Test
    public void equalArrays() {
        int[] intArrayOne = new int[] { 1, 2, 5 }; // 3, sir!
        int[] intArrayTwo = new int[] { 1, 2, 5 };
        assertTrue("int[] should have been equal", Equality.areEqual(intArrayOne, intArrayTwo));
    }
}
