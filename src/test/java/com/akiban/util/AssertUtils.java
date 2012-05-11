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

import org.junit.Assert;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public final class AssertUtils {
    public static void assertCollectionEquals(String message, Collection<?> expected, Collection<?> actual) {
        String expectedString = Strings.join(collectionToString(expected));
        String actualString = Strings.join(collectionToString(actual));
        Assert.assertEquals(message, expectedString, actualString);
        Assert.assertEquals(message, expected, actual);
    }

    public static void assertMapEquals(String message, Map<?,?> expected, Map<?,?> actual) {
        String expectedString = Strings.join(Strings.stringAndSort(expected));
        String actualString = Strings.join(Strings.stringAndSort(actual));
        Assert.assertEquals(message, expectedString, actualString);
        Assert.assertEquals(message, expected, actual);
    }

    public static void assertCollectionEquals(Collection<?> expected, Collection<?> actual) {
        assertCollectionEquals(null, expected, actual);
    }

    private static List<String> collectionToString(Collection<?> expected) {
        if (expected instanceof List)
            return Strings.mapToString(expected);
        else
            return Strings.stringAndSort(expected);
    }
}
