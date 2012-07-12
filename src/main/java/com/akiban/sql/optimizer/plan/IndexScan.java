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

import com.akiban.ais.model.IndexColumn;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;
import com.akiban.util.Strings;
import java.util.*;

public abstract class IndexScan extends BaseScan implements IndexIntersectionNode<ConditionExpression,IndexScan>
{
    public static enum OrderEffectiveness {
        NONE, PARTIAL_GROUPED, GROUPED, SORTED, FOR_MIN_MAX
    }

    private TableSource rootMostTable, rootMostInnerTable, leafMostInnerTable, leafMostTable;

    private boolean covering;

    // Tables that would still need to be fetched if this index were used.
    private Set<TableSource> requiredTables;
    
    // The cost of just the scan of this index, not counting lookups, flattening, etc
    private CostEstimate scanCostEstimate;

    public IndexScan(TableSource table) {
        rootMostTable = rootMostInnerTable = leafMostInnerTable = leafMostTable = table;
    }

    public IndexScan(TableSource rootMostTable, 
                     TableSource rootMostInnerTable,
                     TableSource leafMostInnerTable,
                     TableSource leafMostTable) {
        this.rootMostTable = rootMostTable;
        this.rootMostInnerTable = rootMostInnerTable;
        this.leafMostInnerTable = leafMostInnerTable;
        this.leafMostTable = leafMostTable;
    }

    public TableSource getRootMostTable() {
        return rootMostTable;
    }
    public TableSource getRootMostInnerTable() {
        return rootMostInnerTable;
    }
    public TableSource getLeafMostInnerTable() {
        return leafMostInnerTable;
    }
    public TableSource getLeafMostTable() {
        return leafMostTable;
    }

    /** Return tables included in the index, leafmost to rootmost. */
    public List<TableSource> getTables() {
        List<TableSource> tables = new ArrayList<TableSource>();
        TableSource table = leafMostTable;
        while (true) {
            tables.add(table);
            if (table == rootMostTable) break;
            table = table.getParentTable();
        }
        return tables;
    }

    public boolean isCovering() {
        return covering;
    }
    public void setCovering(boolean covering) {
        this.covering = covering;
    }

    public Set<TableSource> getRequiredTables() {
        return requiredTables;
    }
    public void setRequiredTables(Set<TableSource> requiredTables) {
        this.requiredTables = requiredTables;
    }

    public CostEstimate getScanCostEstimate() {
        return scanCostEstimate;
    }

    public void setScanCostEstimate(CostEstimate scanCostEstimate) {
        this.scanCostEstimate = scanCostEstimate;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (v instanceof ExpressionRewriteVisitor) {
                visitComparands((ExpressionRewriteVisitor)v);
            }
            else if (v instanceof ExpressionVisitor) {
                visitComparands((ExpressionVisitor)v);
            }
            // Don't visit any tables here; they are done by a lookup when that's needed.
        }
        return v.visitLeave(this);
    }
    
    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
    }

    @Override
    public int getPeggedCount() {
        return getNEquality();
    }

    public abstract List<OrderByExpression> getOrdering();
    public abstract OrderEffectiveness getOrderEffectiveness();
    public abstract List<ExpressionNode> getColumns();
    public abstract List<IndexColumn> getIndexColumns();
    public abstract List<ExpressionNode> getEqualityComparands();
    public abstract List<ConditionExpression> getConditions();
    public abstract boolean hasConditions();
    public abstract ExpressionNode getLowComparand();
    public abstract boolean isLowInclusive();
    public abstract ExpressionNode getHighComparand();
    public abstract boolean isHighInclusive();
    public abstract void visitComparands(ExpressionRewriteVisitor v);
    public abstract void visitComparands(ExpressionVisitor v);
    public abstract int getNEquality();
    public abstract boolean isAscendingAt(int index);
    public abstract boolean isRecoverableAt(int index);
    
    @Override
    public String summaryString() {
        return summaryString(-1);
    }
    
    public String summaryString(boolean prettyFormat) {
        return summaryString(prettyFormat ? 0 : -1);
    }

    private String summaryString(int indentation) {
        StringBuilder sb = new StringBuilder();
        buildSummaryString(sb, indentation, true);
        return sb.toString();
    }
     
    protected void buildSummaryString(StringBuilder str, int indentation, boolean full) {
        str.append(super.summaryString());
        str.append('(');
        str.append(summarizeIndex(indentation));
        if (indentation < 0) {
            str.append(", ");
        }
        else if (indentation == 0) {
            str.append(Strings.NL);
            indent(str, 1).append("-> ");
        }
        if (full && covering)
            str.append("covering/");
        if (full && getOrderEffectiveness() != null)
            str.append(getOrderEffectiveness());
        if (full && getOrdering() != null) {
            boolean anyReverse = false, allReverse = true;
            for (int i = 0; i < getOrdering().size(); i++) {
                if (getOrdering().get(i).isAscending() != isAscendingAt(i))
                    anyReverse = true;
                else
                    allReverse = false;
            }
            if (anyReverse) {
                if (allReverse)
                    str.append("/reverse");
                else {
                    for (int i = 0; i < getOrdering().size(); i++) {
                        str.append((i == 0) ? "/" : ",");
                        str.append(getOrdering().get(i).isAscending() ? "ASC" : "DESC");
                    }
                }
            }
        }
        describeEqualityComparands(str);
        if (getLowComparand() != null) {
            str.append(", ");
            str.append((isLowInclusive()) ? ">=" : ">");
            str.append(getLowComparand());
        }
        if (getHighComparand() != null) {
            str.append(", ");
            str.append((isHighInclusive()) ? "<=" : "<");
            str.append(getHighComparand());
        }
        describeConditionRange(str);
        if (full && getCostEstimate() != null) {
            str.append(", ");
            str.append(getCostEstimate());
        }
        str.append(")");
    }
    
    protected static StringBuilder indent(int indentation) {
        return indent(new StringBuilder(), indentation);
    }
    
    protected static StringBuilder indent(StringBuilder str, int indentation) {
        while (indentation --> 0) {
            str.append("    ");
        }
        return str;
    }

    protected abstract String summarizeIndex(int indentation);
    protected void describeConditionRange(StringBuilder output) {}
    protected void describeEqualityComparands(StringBuilder output) {}
}
