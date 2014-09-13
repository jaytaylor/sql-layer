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

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TKeyComparable;

import java.util.List;

/** A join with some kind of hash table loading. */
public class HashJoinNode extends JoinNode
{
    private Joinable loader;
    private BaseHashTable hashTable;
    private List<ExpressionNode> hashColumns, matchColumns;
    private List<TKeyComparable> tKeyComparables;
    private List<AkCollator> collators;

    public HashJoinNode(Joinable loader, Joinable input, Joinable check, JoinType joinType, 
                        BaseHashTable hashTable, List<ExpressionNode> hashColumns, List<ExpressionNode> matchColumns,
                        List<TKeyComparable> tKeyComparables, List<AkCollator> collators) {
        super(input, check, joinType);
        this.loader = loader;
        loader.setOutput(this);
        this.hashTable = hashTable;
        this.hashColumns = hashColumns;
        this.matchColumns = matchColumns;
        this.tKeyComparables = tKeyComparables;
        this.collators = collators;
    }

    public Joinable getLoader() {
        return loader;
    }
    public Joinable getInput() {
        return getLeft();
    }
    public Joinable getCheck() {
        return getRight();
    }

    public BaseHashTable getHashTable() {
        return hashTable;
    }
    public List<ExpressionNode> getHashColumns() {
        return hashColumns;
    }
    public List<ExpressionNode> getMatchColumns() {
        return matchColumns;
    }
    public List<TKeyComparable> getTKeyComparables() {
        return tKeyComparables;
    }
    public List<AkCollator> getCollators() {
        return collators;
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        if (loader == oldInput) {
            loader = (Joinable)newInput;
            loader.setOutput(this);
        }
        super.replaceInput(oldInput, newInput);
    }

    @Override
    protected boolean acceptPlans(PlanVisitor v) {
        return (loader.accept(v) && super.acceptPlans(v));
    }

    @Override
    protected void acceptConditions(PlanVisitor v) {
        super.acceptConditions(v);
        if (v instanceof ExpressionRewriteVisitor) {
            for (int i = 0; i < hashColumns.size(); i++) {
                hashColumns.set(i, hashColumns.get(i).accept((ExpressionRewriteVisitor)v));
                matchColumns.set(i, matchColumns.get(i).accept((ExpressionRewriteVisitor)v));
            }
        }
        else if (v instanceof ExpressionVisitor) {
            for (int i = 0; i < hashColumns.size(); i++) {
                if (!hashColumns.get(i).accept((ExpressionVisitor)v))
                    break;
                if (!matchColumns.get(i).accept((ExpressionVisitor)v))
                    break;
            }
        }
    }

    @Override
    protected void summarizeJoins(StringBuilder str) {
        super.summarizeJoins(str);
        str.append(hashColumns);
        str.append(" = ");
        str.append(matchColumns);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        loader = (Joinable)loader.duplicate(map);
        hashColumns = duplicateList(hashColumns, map);
        matchColumns = duplicateList(matchColumns, map);
    }

}
