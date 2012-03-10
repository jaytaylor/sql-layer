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

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.server.expression.std.Comparison;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class MultiIndexCandidate {
    private Index index;
    private List<ComparisonCondition> pegged;
    private Set<ComparisonCondition> unpegged;

    public MultiIndexCandidate(Index index, Collection<ComparisonCondition> conditions) {
        this.index = index;
        pegged = new ArrayList<ComparisonCondition>(index.getKeyColumns().size());
        unpegged = new HashSet<ComparisonCondition>(conditions);
    }
    
    public MultiIndexCandidate(MultiIndexCandidate copyFrom) {
        this(copyFrom.index, copyFrom.unpegged);
        pegged.addAll(copyFrom.pegged);
    }

    public boolean anyPegged() {
        return ! pegged.isEmpty();
    }
    
    public Collection<ComparisonCondition> findPeggable() {
        List<ComparisonCondition> results = null;
        IndexColumn nextToPeg = nextPegColumn();
        for (ComparisonCondition condition : unpegged) {
            if (canBePegged(condition, nextToPeg)) {
                if (results == null)
                    results = new ArrayList<ComparisonCondition>(unpegged.size());
                results.add(condition);
            }
        }
        return results == null ? Collections.<ComparisonCondition>emptyList() : results;
    }

    public Set<ComparisonCondition> getUnpeggedConditions() {
        return unpegged;
    }
    
    public void peg(ComparisonCondition condition) {
        IndexColumn nextToPeg = nextPegColumn();
        if (!canBePegged(condition, nextToPeg))
            throw new IllegalArgumentException(condition + " can't be pegged to " + this);
        pegged.add(condition);
        boolean removedFromUnpegged = unpegged.remove(condition);
        assert removedFromUnpegged : condition;
    }

    public Index getIndex() {
        return index;
    }

    private IndexColumn nextPegColumn() {
        int alreadyPegged = pegged.size();
        if (alreadyPegged >= index.getKeyColumns().size())
            throw new IllegalStateException("all index conditions are already pegged");
        return index.getKeyColumns().get(alreadyPegged);
    }

    private boolean canBePegged(ComparisonCondition condition, IndexColumn nextToPeg) {
        if (condition.getOperation() == Comparison.EQ) {
            if (condition.getLeft() instanceof ColumnExpression) {
                if (isIndexable(condition.getRight())) {
                    ColumnExpression leftCol = (ColumnExpression) condition.getLeft();
                    if (leftCol.getColumn() == nextToPeg.getColumn())
                        return true;
                    for (ColumnExpression leftEquiv : leftCol.getEquivalents()) {
                        if (leftEquiv.getColumn() == nextToPeg.getColumn())
                            return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean isIndexable(ExpressionNode node) {
        return (node instanceof ConstantExpression) || (node instanceof ParameterExpression);
    }
}
