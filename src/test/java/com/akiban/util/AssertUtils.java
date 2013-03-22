
package com.akiban.util;

import org.junit.Assert;

import java.util.Collection;
import java.util.List;
import java.util.Map;

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
