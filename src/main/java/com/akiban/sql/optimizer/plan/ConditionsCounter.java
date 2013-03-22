
package com.akiban.sql.optimizer.plan;

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
