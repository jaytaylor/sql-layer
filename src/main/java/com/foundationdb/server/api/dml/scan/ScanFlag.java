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

package com.foundationdb.server.api.dml.scan;

import java.util.EnumSet;
import java.util.Set;

import com.foundationdb.util.ArgumentValidation;

public enum ScanFlag {
    DESCENDING (0),
    START_RANGE_EXCLUSIVE(1),
    END_RANGE_EXCLUSIVE(2),
    SINGLE_ROW(3),
    LEXICOGRAPHIC(4),
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

    public static int addFlag(int flagsInt, ScanFlag flag) {
        return flagsInt | (1 << flag.position);
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
