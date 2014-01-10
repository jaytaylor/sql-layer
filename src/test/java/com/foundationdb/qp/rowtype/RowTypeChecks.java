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

package com.foundationdb.qp.rowtype;

import com.foundationdb.server.types.TInstance;

import java.util.Arrays;

import static org.junit.Assert.*;

public final class RowTypeChecks {

    public static void checkRowTypeFields(String message, RowType rowType, TInstance... expected) {
        assertEquals (rowType.nFields(), expected.length);
        TInstance[] actual = new TInstance[rowType.nFields()];
        for (int i = 0; i < actual.length; ++i) {
            actual[i] = rowType.typeAt(i);
        }
        
        if (!Arrays.equals(expected, actual)) {
            assertEquals (message, Arrays.toString(expected), Arrays.toString(actual));
            assertArrayEquals(message, expected, actual);
        }
    }
}
