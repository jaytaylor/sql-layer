
package com.akiban.server.types;

import com.akiban.util.ArgumentValidation;

public final class ValueSourceHelper {

    public static void checkType(AkType expected, AkType actual) {
        ArgumentValidation.notNull("expected", expected);
        if (expected == AkType.UNSUPPORTED) {
            throw new IllegalStateException(
                    "expected UNSUPPORTED type; conversion source/target probably not initialized correctly"
            );
        }

        if (actual == AkType.NULL) {
            return; // always valid!
        }
        if (expected != actual) {
            throw new WrongValueGetException(expected, actual);
        }
    }

    private ValueSourceHelper() {}
}
