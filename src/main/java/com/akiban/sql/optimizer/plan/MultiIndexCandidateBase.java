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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.util.Equality;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class MultiIndexCandidateBase<S> {
    private Index index;
    private List<S> pegged;
    private Set<S> unpegged;

    public MultiIndexCandidateBase(Index index, Collection<S> conditions) {
        this.index = index;
        pegged = new ArrayList<S>(index.getKeyColumns().size());
        unpegged = new HashSet<S>(conditions);
    }
    
    public void pegAll(List<? extends S> conditions) {
        for (S condition : conditions)
            peg(condition, false);
    }

    public boolean anyPegged() {
        return ! pegged.isEmpty();
    }
    
    public Set<S> getUnpeggedCopy() {
        return new HashSet<S>(unpegged);
    }
    
    public Collection<S> findPeggable() {
        List<S> results = null;
        IndexColumn nextToPeg = nextPegColumn();
        for (S condition : unpegged) {
            if (canBePegged(condition, nextToPeg)) {
                if (results == null)
                    results = new ArrayList<S>(unpegged.size());
                results.add(condition);
            }
        }
        return results == null ? Collections.<S>emptyList() : results;
    }

    public List<S> getPegged() {
        return pegged;
    }
    
    public void peg(S condition) {
        peg(condition, true);
    }
    
    public void peg(S condition, boolean checkIfInUnpegged) {
        IndexColumn nextToPeg = nextPegColumn();
        if (!canBePegged(condition, nextToPeg))
            throw new IllegalArgumentException(condition + " can't be pegged to " + this);
        pegged.add(condition);
        boolean removedFromUnpegged = unpegged.remove(condition);
        assert (!checkIfInUnpegged) || removedFromUnpegged : condition;
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
    
    protected abstract boolean columnsMatch(S condition, Column column);
    

    private boolean canBePegged(S condition, IndexColumn nextToPeg) {
        Column nextToPegColumn = nextToPeg.getColumn();
        return columnsMatch(condition, nextToPegColumn);
        //        if (condition.getOperation() == Comparison.EQ) {
//            if (condition.getLeft() instanceof ColumnExpression) {
//                if (isIndexable(condition.getRight())) {
//                    ColumnExpression leftCol = (ColumnExpression) condition.getLeft();
//                    if (leftCol.getColumn() == nextToPeg.getColumn())
//                        return true;
//                    for (ColumnExpression leftEquiv : leftCol.getEquivalents()) {
//                        if (leftEquiv.getColumn() == nextToPeg.getColumn())
//                            return true;
//                    }
//                }
//            }
//        }
//        return false;
    }
//
//    private boolean isIndexable(ExpressionNode node) {
//        return (node instanceof ConstantExpression) || (node instanceof ParameterExpression);
//    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder().append(index).append(" pegged to ");
        if (pegged == null || pegged.isEmpty())
            sb.append("nothing");
        else
            sb.append(pegged);
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MultiIndexCandidateBase<?> other = (MultiIndexCandidateBase<?>) o;

        return Equality.areEqual(index, other.index) && Equality.areEqual(pegged, other.pegged);

    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + (pegged != null ? pegged.hashCode() : 0);
        return result;
    }
}
