
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

import java.util.List;


/** Application of a Hash Table. */
public class HashTableLookup extends BasePlanNode
{
    private HashTable hashTable;
    private List<ExpressionNode> lookupExpressions;
    private PlanNode check;
    private boolean isOuterLeftJoin;

    public HashTableLookup(HashTable hashTable,
                           List<ExpressionNode> lookupExpressions,
                           boolean isOuterLeftJoin){
        this.hashTable = hashTable;
        this.lookupExpressions = lookupExpressions;
        this.isOuterLeftJoin = isOuterLeftJoin;

    }

    public HashTable getHashTable() {
        return hashTable;
    }
    public List<ExpressionNode> getLookupExpressions() {
        return lookupExpressions;
    }
    public PlanNode getCheck() {
        return check;
    }
    public boolean isOuterLeftJoin(){
        return isOuterLeftJoin;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)){
                if (v instanceof ExpressionRewriteVisitor) {
                    for (int i = 0; i < lookupExpressions.size(); i++) {
                        lookupExpressions.set(i, lookupExpressions.get(i).accept((ExpressionRewriteVisitor)v));
                    }
                }
                else if (v instanceof ExpressionVisitor) {
                    for (ExpressionNode expr : lookupExpressions) {
                        if (!expr.accept((ExpressionVisitor)v))
                            break;
                    }
                }
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        str.append(hashTable);
        str.append(", ");
        str.append(lookupExpressions);
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        check = (PlanNode)check.duplicate(map);
    }
}
