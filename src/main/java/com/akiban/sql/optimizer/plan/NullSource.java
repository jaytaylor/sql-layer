
package com.akiban.sql.optimizer.plan;

/** A source that never outputs any rows. */
public class NullSource extends BaseJoinable implements ColumnSource
{
    public NullSource() {
    }

    @Override
    public String getName() {
        return "NULL";
    }

    @Override
    public boolean isTable() {
        return false;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        return v.visit(this);
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

}
