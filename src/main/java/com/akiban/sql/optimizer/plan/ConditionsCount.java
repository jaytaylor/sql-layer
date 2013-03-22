
package com.akiban.sql.optimizer.plan;

public interface ConditionsCount<C> {
    HowMany getCount(C condition);
    
    enum HowMany {
        NONE, ONE, MANY
    }
}
