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

package com.foundationdb.sql.optimizer.plan;

import java.util.*;

public abstract class BaseDuplicatable implements Duplicatable, Cloneable
{
    @Override
    public final Duplicatable duplicate() {
        return duplicate(new DuplicateMap());
    }

    protected boolean maintainInDuplicateMap() {
        return false;
    }

    @Override
    public Duplicatable duplicate(DuplicateMap map) {
        BaseDuplicatable copy;
        try {
            if (maintainInDuplicateMap()) {
                copy = map.get(this);
                if (copy != null)
                    return copy;
                copy = (BaseDuplicatable)clone();
                map.put(this, copy);
            }
            else
                copy = (BaseDuplicatable)clone();
        }
        catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }
        copy.deepCopy(map);
        return copy;
    }

    /** Deep copy all the fields, using the given map. */
    protected void deepCopy(DuplicateMap map) {
    }

    @SuppressWarnings("unchecked")
    protected static <T extends Duplicatable> List<T> duplicateList(List<T> list,
                                                                    DuplicateMap map) {
        List<T> copy = new ArrayList<>(list.size());
        for (T elem : list) {
            copy.add((T)elem.duplicate(map));
        }
        return copy;
    }

    @SuppressWarnings("unchecked")
    protected static <T extends Duplicatable> Set<T> duplicateSet(Set<T> set,
                                                                  DuplicateMap map) {
        Set<T> copy = new HashSet<>(set.size());
        for (T elem : set) {
            copy.add((T)elem.duplicate(map));
        }
        return copy;
    }

}
