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

package com.foundationdb.server.types;

import com.foundationdb.util.ArgumentValidation;

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
