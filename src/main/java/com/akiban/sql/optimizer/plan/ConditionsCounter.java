/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
        counter = new HashMap<C, HowMany>(capacity);
    }

    // mapping of C -> [0, 1, >1 ]
    private Map<C,HowMany> counter;
}
