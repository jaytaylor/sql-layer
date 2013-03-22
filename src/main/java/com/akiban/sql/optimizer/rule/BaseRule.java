
package com.akiban.sql.optimizer.rule;

import org.slf4j.Logger;

public abstract class BaseRule
{
    protected BaseRule() {
    }

    public String getName() {
        return getClass().getSimpleName();
    }

    protected abstract Logger getLogger();

    public abstract void apply(PlanContext plan);
}
