package com.akiban.cserver.api.dml.scan;

import com.akiban.util.ArgumentValidation;

import java.util.EnumSet;
import java.util.Set;

public enum ScanFlag {
    DESCENDING (0),
    START_RANGE_EXCLUSIVE(1),
    END_RANGE_EXCLUSIVE(2),
    SINGLE_ROW(3),
    MATCH_BY_PREFIX(4),
    START_AT_BEGINNING(5),
    END_AT_END(6),
    DEEP(7)
    ;

    private final int position;

    ScanFlag(int position) {
        this.position = position;
    }

    int getPosition() {
        return position;
    }

    public static int toRowDataFormat(Set<ScanFlag> flags) {
        int result = 0;
        for (ScanFlag flag : flags) {
            result |= 1 << flag.position;
        }
        return result;
    }

    public static EnumSet<ScanFlag> fromRowDataFormat(int packed) {
        ArgumentValidation.isNotNegative("packed int", packed);
        ArgumentValidation.isLT("packed int", packed, 1 << values().length );
        EnumSet<ScanFlag> retval = EnumSet.noneOf(ScanFlag.class);
        for (ScanFlag flag : ScanFlag.values()) {
            final int value = 1 << flag.position;
            if ((packed & value) == value) {
                retval.add(flag);
            }
        }
        return retval;
    }
}
