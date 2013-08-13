/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.types3.pvalue;

import com.akiban.server.types.AkType;

public enum PUnderlying {
    BOOL, INT_8, INT_16, UINT_16, INT_32, INT_64, FLOAT, DOUBLE, BYTES, STRING
    ;

    public static PUnderlying valueOf(AkType akType) {
        switch (akType) {
        case INT:
            return PUnderlying.INT_64;
        case VARCHAR:
            return PUnderlying.BYTES;
        default:
            throw new AssertionError(akType);
        }
    }
}
