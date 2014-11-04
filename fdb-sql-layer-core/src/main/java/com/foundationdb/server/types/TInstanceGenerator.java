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

import java.util.Arrays;

public final class TInstanceGenerator {
    public TInstance setNullable(boolean isNullable) {
        switch (attrs.length) {
        case 0:
            return tclass.instance(isNullable);
        case 1:
            return tclass.instance(attrs[0], isNullable);
        case 2:
            return tclass.instance(attrs[0], attrs[1], isNullable);
        case 3:
            return tclass.instance(attrs[0], attrs[1], attrs[2], isNullable);
        case 4:
            return tclass.instance(attrs[0], attrs[1], attrs[2], attrs[3], isNullable);
        default:
            throw new AssertionError("too many attrs!: " + Arrays.toString(attrs) + " with " + tclass);
        }
    }

    int[] attrs() {
        return Arrays.copyOf(attrs, attrs.length);
    }

    TClass tClass() {
        return tclass;
    }

    public String toString(boolean useShorthand) {
        return setNullable(true).toStringIgnoringNullability(useShorthand);
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public TInstanceGenerator(TClass tclass, int... attrs) {
        this.tclass = tclass;
        this.attrs = Arrays.copyOf(attrs, attrs.length);
    }
    private final TClass tclass;

    private final int[] attrs;
}
