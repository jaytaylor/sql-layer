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

package com.akiban.server.api.dml;

import java.util.HashSet;
import java.util.Set;

public final class EasyUseColumnSelector implements ColumnSelector {

    private final Set<Integer> set;

    public EasyUseColumnSelector(Set<Integer> set) {
        this.set = new HashSet<Integer>(set);
    }

    public EasyUseColumnSelector(int... selectedPositions) {
        set = new HashSet<Integer>();
        for (int i : selectedPositions) {
            set.add(i);
        }
    }

    @Override
    public boolean includesColumn(int columnPosition) {
        return set.contains(columnPosition);
    }
}
