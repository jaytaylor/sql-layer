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

package com.akiban.qp.rowtype;

import com.akiban.server.types.AkType;

import java.util.Arrays;

import static org.junit.Assert.*;

public final class RowTypeChecks {
    public static void checkRowTypeFields(String message, RowType rowType, AkType... expected) {
        AkType[] actual = new AkType[rowType.nFields()];
        for (int i=0; i < actual.length; ++i) {
            actual[i] = rowType.typeAt(i);
        }
        if (!Arrays.equals(expected, actual)) {
            // I like the toString representation more, so check that first
            assertEquals(message, Arrays.toString(expected), Arrays.toString(actual));
            assertArrayEquals(message, expected, actual);
        }
    }

    public static void checkRowTypeFields(RowType rowType, AkType... expected) {
        checkRowTypeFields(null, rowType, expected);
    }
}
