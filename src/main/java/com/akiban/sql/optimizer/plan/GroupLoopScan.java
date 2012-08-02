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
