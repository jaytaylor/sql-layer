
package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.TCastPath;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;

public final class Paths {

    public static final TCastPath INTEGERS = TCastPath.create(
            MNumeric.TINYINT_UNSIGNED,
            MNumeric.INT,
            MNumeric.BIGINT
    );

    private Paths() {}
}
