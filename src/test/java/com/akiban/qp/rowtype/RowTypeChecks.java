
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
