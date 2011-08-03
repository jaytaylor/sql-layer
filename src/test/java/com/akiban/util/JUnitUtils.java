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

import java.util.Collection;

import static org.junit.Assert.assertEquals;

public final class JUnitUtils {

    public static void equalCollections(String message, Collection<?> expected, Collection<?> actual) {
        if (expected == null) {
            assertEquals(message, expected, actual);
        }
        else if (!expected.equals(actual)) {
            assertEquals(message, Strings.join(expected), Strings.join(actual));
            assertEquals(message, expected, actual);
        }
    }

    private JUnitUtils() {}
}
