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

import java.util.Collection;
import java.util.List;

public class GroupLoopScan extends BaseScan
{
    private TableSource insideTable, outsideTable;
    private boolean insideParent;
    private List<ComparisonCondition> joinConditions;

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

    @Override
    public Collection<? extends ConditionExpression> getConditions() {
        return getJoinConditions();
    }

    @Override
    public void visitComparands(ExpressionRewriteVisitor v) {
    }

    @Override
    public void visitComparands(ExpressionVisitor v) {
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

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            // Don't own tables, right?
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString(PlanToString.Configuration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
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
