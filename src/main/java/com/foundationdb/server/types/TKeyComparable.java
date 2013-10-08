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

public final class TKeyComparable {

    public TClass getLeftTClass() {
        return leftTClass;
    }

    public TClass getRightTClass() {
        return rightTClass;
    }

    public TComparison getComparison() {
        return comparison;
    }

    public TKeyComparable(TClass leftClass, TClass rightClass, TComparison comparison) {
        this.leftTClass = leftClass;
        this.rightTClass = rightClass;
        this.comparison = comparison;
    }

    private final TClass leftTClass;
    private final TClass rightTClass;
    private final TComparison comparison;
}
