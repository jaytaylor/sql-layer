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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class ConditionsCounter<C> implements ConditionsCount<C> {
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
        HowMany howMany = getCount(condition);
        switch (howMany) {
        case NONE:
            counter.put(condition, HowMany.ONE);
            break;
        case ONE:
            counter.put(condition, HowMany.MANY);
            break;
        case MANY:
            break;
        default:
            throw new AssertionError(howMany.name());
        }
    }

    @Override
    public HowMany getCount(C condition) {
        HowMany internalCount = counter.get(condition);
        return internalCount == null ? HowMany.NONE : internalCount;
    }

    public ConditionsCounter(int capacity) {
        counter = new HashMap<>(capacity);
    }

    // mapping of C -> [0, 1, >1 ]
    private Map<C,HowMany> counter;
}
