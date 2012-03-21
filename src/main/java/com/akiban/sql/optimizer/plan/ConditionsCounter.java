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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class ConditionsCounter<C> {
    public void clear() {
        counter.clear();
    }
    
    public int conditionsCounted() {
        return counter.size();
    }
    
    public Set<C> getCountedConditions() {
        return counter.keySet();
    }
    
    public void increment(C condition) {
        Boolean prev = counter.get(condition);
        if (prev == ZERO)
            counter.put(condition, ONE);
        else if (prev == ONE)
            counter.put(condition, MANY);
    }
    
    public boolean exactlyOne(C condition) {
        return ONE.equals(counter.get(condition));
    }

    public ConditionsCounter(int capacity) {
        counter = new HashMap<C, Boolean>(capacity);
    }

    // mapping of C -> [0, 1, >1 ]
    private Map<C,Boolean> counter;
    private static final Boolean ZERO = null; // must be null, since it's what Map.get returns when there's no mapping
    private static final Boolean ONE = Boolean.TRUE;
    private static final Boolean MANY = Boolean.FALSE;
}
