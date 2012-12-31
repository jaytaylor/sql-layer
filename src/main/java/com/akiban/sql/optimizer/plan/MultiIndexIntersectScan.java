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
import com.akiban.ais.model.UserTable;
import com.akiban.util.Strings;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;

import java.util.List;

public final class MultiIndexIntersectScan extends IndexScan {
    
    private IndexScan outputScan;
    private IndexScan selectorScan;
    private int comparisonColumns;
    private List<ConditionExpression> conditions;

    public MultiIndexIntersectScan(IndexScan outerScan, IndexScan selectorScan, int comparisonColumns)
    {
        this(outerScan.getRootMostTable(), outerScan.getLeafMostTable());
        assert outerScan.getRootMostTable() == outerScan.getRootMostInnerTable() : outerScan;
        assert outerScan.getLeafMostTable() == outerScan.getLeafMostInnerTable() : outerScan;
        this.outputScan = outerScan;
        this.selectorScan = selectorScan;
        this.comparisonColumns = comparisonColumns;
    }
    
    private MultiIndexIntersectScan(TableSource rootMost, TableSource leafMost) {
        super(rootMost, rootMost, leafMost, leafMost);
    }
    
    public IndexScan getOutputIndexScan() {
        return outputScan;
    }
    
    public IndexScan getSelectorIndexScan() {
        return selectorScan;
    }

    public int getComparisonFields() {
        return comparisonColumns;
    }

    public boolean[] getComparisonFieldDirections() {
        boolean[] ascending = new boolean[comparisonColumns];
        for (int i = 0; i < comparisonColumns; i++) {
            ascending[i] = getOrdering().get(getPeggedCount() + i).isAscending();
        }
        return ascending;
    }

    public int getOutputOrderingFields() {
        return getOrderingFields(outputScan);
    }

    public int getSelectorOrderingFields() {
        return getOrderingFields(selectorScan);
    }

    private int getOrderingFields(IndexScan scan) {
        return scan.getAllColumns().size() - scan.getPeggedCount();
    }

    @Override
    public List<OrderByExpression> getOrdering() {
        return outputScan.getOrdering();
    }

    @Override
    public OrderEffectiveness getOrderEffectiveness() {
        return outputScan.getOrderEffectiveness();
    }

    @Override
    public List<ExpressionNode> getEqualityComparands() {
        return outputScan.getEqualityComparands();
    }

    @Override
    public ExpressionNode getLowComparand() {
        return outputScan.getLowComparand();
    }

    @Override
    public boolean isLowInclusive() {
        return outputScan.isLowInclusive();
    }

    @Override
    public ExpressionNode getHighComparand() {
        return outputScan.getHighComparand();
    }

    @Override
    public boolean isHighInclusive() {
        return outputScan.isHighInclusive();
    }

    @Override
    public List<ConditionExpression> getConditions() {
        return conditions;
    }

    @Override
    public boolean hasConditions() {
        return !conditions.isEmpty();
    }

    public void setConditions(List<ConditionExpression> conditions) {
        this.conditions = conditions;
    }

    @Override
    public List<ExpressionNode> getColumns() {
        return outputScan.getColumns();
    }

    @Override
    public UserTable getLeafMostUTable() {
        return outputScan.getLeafMostTable().getTable().getTable();
    }

    @Override
    public List<IndexColumn> getAllColumns() {
        return outputScan.getAllColumns();
    }

    @Override
    public int getNEquality() {
        return outputScan.getNEquality();
    }

    @Override
    public void incrementConditionsCounter(ConditionsCounter<? super ConditionExpression> counter) {
        outputScan.incrementConditionsCounter(counter);
        selectorScan.incrementConditionsCounter(counter);
    }

    @Override
    public boolean isUseful(ConditionsCount<? super ConditionExpression> count) {
        return outputScan.isUseful(count) && selectorScan.isUseful(count);
    }

    @Override
    public List<IndexColumn> getIndexColumns() {
        return outputScan.getIndexColumns();
    }

    @Override
    public int getNKeyColumns() {
        return outputScan.getNKeyColumns();
    }

    @Override
    public boolean usesAllColumns() {
        return outputScan.usesAllColumns();
    }

    @Override
    public void setUsesAllColumns(boolean usesAllColumns) {
        outputScan.setUsesAllColumns(usesAllColumns);
    }

    @Override
    protected String summarizeIndex(int indentation) {
        boolean pretty = indentation >= 0;
        int nextIndentation = pretty ? indentation + 1 : -1;

        StringBuilder sb = new StringBuilder();
        if (pretty) {
            sb.append("compare ").append(getComparisonFields()).append(Strings.NL);
            indent(sb, nextIndentation);
        }
        else {
            sb.append("INTERSECT(compare ").append(getComparisonFields()).append(", ");
        }
        summarizeChildIndex(outputScan, nextIndentation, sb);
        if (pretty) {
            sb.append(Strings.NL);
            indent(sb, nextIndentation);
        }
        else {
            sb.append(" AND ");
        }
        summarizeChildIndex(selectorScan, nextIndentation, sb);
        if (!pretty)
            sb.append(')');
        return sb.toString();
    }

    private void summarizeChildIndex(IndexScan child, int indentation, StringBuilder sb) {
        int skips = child.getAllColumns().size() - getOrderingFields(child);
        sb.append("skip ").append(skips).append(": ");
        child.buildSummaryString(sb, indentation, false);
    }

    @Override
    public boolean isAscendingAt(int i) {
        return outputScan.isAscendingAt(i);
    }

    @Override
    public boolean isRecoverableAt(int i) {
        return outputScan.isRecoverableAt(i);
    }

    @Override
    public UserTable findCommonAncestor(IndexScan other) {
        return outputScan.findCommonAncestor(other);
    }

    @Override
    public void visitComparands(ExpressionRewriteVisitor v) {
        outputScan.visitComparands(v);
        selectorScan.visitComparands(v);
    }

    @Override
    public void visitComparands(ExpressionVisitor v) {
        outputScan.visitComparands(v);
        selectorScan.visitComparands(v);
    }

}
