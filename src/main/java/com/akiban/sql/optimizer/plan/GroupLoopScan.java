
package com.akiban.sql.optimizer.plan;

import java.util.List;
import java.util.Set;

public class GroupLoopScan extends BaseScan
{
    private TableSource insideTable, outsideTable;
    private boolean insideParent;
    private List<ComparisonCondition> joinConditions;
    private Set<TableSource> requiredTables;

    public GroupLoopScan(TableSource insideTable, TableSource outsideTable,
                         boolean insideParent, 
                         List<ComparisonCondition> joinConditions) {
        this.insideTable = insideTable;
        this.outsideTable = outsideTable;
        this.insideParent = insideParent;
        this.joinConditions = joinConditions;
    }

    public TableSource getInsideTable() {
        return insideTable;
    }
    public TableSource getOutsideTable() {
        return outsideTable;
    }

    public boolean isInsideParent() {
        return insideParent;
    }

    public List<ComparisonCondition> getJoinConditions() {
        return joinConditions;
    }

    public ColumnExpression getOutsideJoinColumn() {
        ComparisonCondition joinCondition = joinConditions.get(0);
        ColumnExpression joinColumn = (ColumnExpression)joinCondition.getLeft();
        if (joinColumn.getTable() == outsideTable)
            return joinColumn;
        joinColumn = (ColumnExpression)joinCondition.getRight();
        assert (joinColumn.getTable() == outsideTable);
        return joinColumn;
    }

    public Set<TableSource> getRequiredTables() {
        return requiredTables;
    }
    public void setRequiredTables(Set<TableSource> requiredTables) {
        this.requiredTables = requiredTables;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            // Don't own tables, right?
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append('(');
        str.append(outsideTable.getName());
        str.append(" - ");
        str.append(insideTable.getName());
        if (getCostEstimate() != null) {
            str.append(", ");
            str.append(getCostEstimate());
        }
        str.append(")");
        return str.toString();
    }

}
