
package com.akiban.sql.optimizer.plan;

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

    protected static <T extends Duplicatable> List<T> duplicateList(List<T> list,
                                                                    DuplicateMap map) {
        List<T> copy = new ArrayList<>(list.size());
        for (T elem : list) {
            copy.add((T)elem.duplicate(map));
        }
        return copy;
    }

    protected static <T extends Duplicatable> Set<T> duplicateSet(Set<T> set,
                                                                  DuplicateMap map) {
        Set<T> copy = new HashSet<>(set.size());
        for (T elem : set) {
            copy.add((T)elem.duplicate(map));
        }
        return copy;
    }

}
