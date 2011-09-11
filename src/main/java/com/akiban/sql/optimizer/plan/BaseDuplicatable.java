/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.plan;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

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

    protected static <T extends Duplicatable> List<T> duplicateList(List<T> list,
                                                                    DuplicateMap map) {
        List<T> copy = new ArrayList<T>(list);
        for (int i = 0; i < copy.size(); i++) {
            copy.set(i, (T)copy.get(i).duplicate(map));
        }
        return copy;
    }

}
