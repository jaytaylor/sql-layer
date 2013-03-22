
package com.akiban.sql.optimizer.plan;

import java.util.List;

public abstract class BaseLookup extends BasePlanWithInput implements TableLoader
{
    private List<TableSource> tables;

    public BaseLookup(PlanNode input, List<TableSource> tables) {
        super(input);
        this.tables = tables;
    }

    /** The tables that this lookup introduces into the stream. */
    @Override
    public List<TableSource> getTables() {
        return tables;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if ((getInput() == null) || getInput().accept(v)) {
                for (TableSource table : tables) {
                    if (!table.accept(v))
                        break;
                }
            }
        }
        return v.visitLeave(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        tables = duplicateList(tables, map);
    }

}
