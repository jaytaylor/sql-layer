/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.api.dml.scan;

import java.util.EnumSet;
import java.util.Set;

import com.akiban.util.ArgumentValidation;

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
