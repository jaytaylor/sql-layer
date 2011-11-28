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

import org.junit.Assert;

import java.util.Collection;

public final class AssertUtils {
    public static void assertCollectionEquals(String message, Collection<?> expected, Collection<?> actual) {
        Assert.assertEquals(message, Strings.join(expected), Strings.join(actual));
        Assert.assertEquals(message, expected, actual);
    }

    public static void assertCollectionEquals(Collection<?> expected, Collection<?> actual) {
        assertCollectionEquals(null, expected, actual);
    }
}
