/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.util;

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
