
package com.akiban.server.api.dml;

import java.util.HashSet;
import java.util.Set;

public final class SetColumnSelector implements ColumnSelector {

    private final Set<Integer> set;

    public SetColumnSelector(Set<Integer> set) {
        this.set = new HashSet<>(set);
    }

    public SetColumnSelector(int... selectedPositions) {
        set = new HashSet<>();
        for (int i : selectedPositions) {
            set.add(i);
        }
    }

    @Override
    public boolean includesColumn(int columnPosition) {
        return set.contains(columnPosition);
    }
}
