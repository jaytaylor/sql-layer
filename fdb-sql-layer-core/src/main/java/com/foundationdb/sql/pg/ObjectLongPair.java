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

package com.foundationdb.sql.pg;

public class ObjectLongPair {
    public final Object obj;
    public final long longVal;

    public ObjectLongPair(Object obj, long longVal) {
        assert obj != null : "Null obj for longVal " + longVal;
        this.obj = obj;
        this.longVal = longVal;
    }

    @Override
    public String toString() {
        return "[" + obj + "," + longVal + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ObjectLongPair rhs = (ObjectLongPair)o;
        return (longVal == rhs.longVal) && obj.equals(rhs.obj);
    }

    @Override
    public int hashCode() {
        int result = obj.hashCode();
        result = 31 * result + (int)(longVal ^ (longVal >>> 32));
        return result;
    }
}
