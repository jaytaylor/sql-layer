/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.query;

import com.akiban.sql.StandardException;

import java.util.List;

/** A query / subquery */
public class Query
{
    private BaseJoinNode joins;
    private List<ResultExpression> results;
    private List<BooleanExpression> conditions;
    private List<GroupByExpression> groupBy;
    private List<BooleanExpression> having;
    private List<OrderByExpression> orderBy;
    private int offset = 0, limit = -1;
    private boolean offsetIsParameter = false, limitIsParameter = false;

    public Query(BaseJoinNode joins, List<ResultExpression> results) {
        this.joins = joins;
        this.results = results;
    }

    public BaseJoinNode getJoins() {
        return joins;
    }
    
    public List<ResultExpression> getResults() {
        return results;
    }

    public List<BooleanExpression> getConditions() {
        return conditions;
    }
    public void setConditions(List<BooleanExpression> conditions) {
        this.conditions = conditions;
    }
    
    public List<GroupByExpression> getGroupBy() {
        return groupBy;
    }
    public void setGroupBy(List<GroupByExpression> groupBy) {
        this.groupBy = groupBy;
    }

    public List<BooleanExpression> getHaving() {
        return having;
    }
    public void setHaving(List<BooleanExpression> having) {
        this.having = having;
    }

    public List<OrderByExpression> getOrderBy() {
        return orderBy;
    }
    public void setOrderBy(List<OrderByExpression> orderBy) {
        this.orderBy = orderBy;
    }

    public int getOffset() {
        return offset;
    }
    public boolean isOffsetParameter() {
        return offsetIsParameter;
    }
    public int getLimit() {
        return limit;
    }
    public boolean isLimitParameter() {
        return limitIsParameter;
    }
    public void setOffsetAndLimit(int offset, boolean offsetIsParameter,
                                  int limit, boolean limitIsParameter) {
        this.offset = offset;
        this.offsetIsParameter = offsetIsParameter;
        this.limit = limit;
        this.limitIsParameter = limitIsParameter;
    }


}
