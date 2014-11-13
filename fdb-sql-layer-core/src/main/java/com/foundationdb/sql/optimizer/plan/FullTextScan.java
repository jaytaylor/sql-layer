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

import com.foundationdb.ais.model.FullTextIndex;

import java.util.List;

public class FullTextScan extends BaseScan
{
    private FullTextIndex index;
    private FullTextQuery query;
    private int limit;
    private TableSource indexTable;
    private List<ConditionExpression> conditions;

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

    @Override
    public void visitComparands(ExpressionRewriteVisitor v) {
    }

    @Override
    public void visitComparands(ExpressionVisitor v) {
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
