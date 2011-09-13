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

package com.akiban.sql.optimizer.plan;

import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;

import com.akiban.ais.model.Index;

import java.util.*;

public class IndexUsage extends BaseDuplicatable
{
    private Index index;

    // Conditions subsumed by this index.
    // TODO: any cases where a condition is only partially handled and
    // still needs to be checked with Select?
    private List<ConditionExpression> conditions;

    // First equalities in the order of the index.
    private List<ExpressionNode> equalityComparands;
    // Followed by an optional inequality.
    private ExpressionNode lowComparand, highComparand;
    // TODO: This doesn't work for merging: consider x < ? AND x <= ?. 
    // May need building of index keys in the expressions subsystem.
    private boolean lowInclusive, highInclusive;

    // This is how the indexed result will be ordered from using this index.
    // TODO: Is this right? Are we allowed to switch directions between segments?
    private List<OrderByExpression> ordering;

    public IndexUsage(Index index) {
        this.index = index;
    }

    public Index getIndex() {
        return index;
    }

    public List<ConditionExpression> getConditions() {
        return conditions;
    }

    public List<ExpressionNode> getEqualityComparands() {
        return equalityComparands;
    }
    public ExpressionNode getLowComparand() {
        return lowComparand;
    }
    publc boolean isLowInclusive() {
        return lowInclusive;
    }
    public ExpressionNode getHighComparand() {
        return highComparand;
    }
    publc boolean isHighInclusive() {
        return highInclusive;
    }

    public void setConditions(List<ConditionExpression> conditions,
                              List<ExpressionNode> equalityComparands,
                              ExpressionNode lowComparand, boolean lowInclusive,
                              ExpressionNode highComparand, boolean highInclusive) {
        this.conditions = conditions;
        this.equalityComparands = equalityComparands;
        this.lowComparand = lowComparand;
        this.lowInclusive = lowInclusive;
        this.highComparand = highComparand;
        this.highInclusive = highInclusive;
    }

    public List<OrderByExpression> getOrdering() {
        return ordering;
    }
    public void setOrdering(List<OrderByExpression> ordering) {
        this.ordering = ordering;
    }
                              
    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        equalityComparands = duplicateList(equalityComparands, map);
        if (lowComparand != null)
            lowComparand = (ConditionExpression)lowComparand.duplicate(map);
        if (highComparand != null)
            highComparand = (ConditionExpression)highComparand.duplicate(map);
        ordering = duplicateList(ordering, map);
    }
    
}
