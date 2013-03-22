
package com.akiban.sql.optimizer.plan;

import java.util.List;

/** Some kind of hash table for joining, etc. */
public abstract class BaseHashTable extends BasePlanElement
{
    protected BaseHashTable() {
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "@" + Integer.toString(hashCode(), 16);
    }
}
