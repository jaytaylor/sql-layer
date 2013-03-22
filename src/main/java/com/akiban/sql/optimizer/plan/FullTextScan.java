
package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.FullTextIndex;

import java.util.List;
import java.util.Set;

public class FullTextScan extends BaseScan
{
    private FullTextIndex index;
    private FullTextQuery query;
    private int limit;
    private TableSource indexTable;
    private List<ConditionExpression> conditions;
    private Set<TableSource> requiredTables;

    public FullTextScan(FullTextIndex index, FullTextQuery query,
                        TableSource indexTable, List<ConditionExpression> conditions) {
        this.index = index;
        this.query = query;
        this.indexTable = indexTable;
        this.conditions = conditions;
    }

    public FullTextIndex getIndex() {
        return index;
    }

    public FullTextQuery getQuery() {
        return query;
    }

    public int getLimit() {
        return limit;
    }
    public void setLimit(int limit) {
        this.limit = limit;
    }

    public TableSource getIndexTable() {
        return indexTable;
    }

    public List<ConditionExpression> getConditions() {
        return conditions;
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
        str.append(indexTable.getName());
        str.append(" - ");
        str.append(query);
        if (limit > 0) {
            str.append(" LIMIT ");
            str.append(limit);
        }
        str.append(")");
        return str.toString();
    }

}
