
package com.akiban.sql.optimizer.plan;

public abstract class BaseJoinable extends BasePlanNode implements Joinable
{
    protected BaseJoinable() {
    }

    @Override
    public boolean isTable() {
        return false;
    }
    @Override
    public boolean isGroup() {
        return false;
    }
    @Override
    public boolean isJoin() {
        return false;
    }
    @Override
    public boolean isInnerJoin() {
        return false;
    }
}
