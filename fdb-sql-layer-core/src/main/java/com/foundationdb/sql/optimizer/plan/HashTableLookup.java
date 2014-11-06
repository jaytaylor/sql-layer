
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

import java.util.Collection;
import java.util.List;

/** Application of a Hash Table. */
public class HashTableLookup extends BaseJoinable
{
    private HashTable hashTable;
    private List<ExpressionNode> lookupExpressions;
    private List<ConditionExpression> conditions;
    private Collection<? extends ColumnSource> tables;

    public HashTableLookup(HashTable hashTable,
                           List<ExpressionNode> lookupExpressions,
                           List<ConditionExpression> conditions,
                           Collection<? extends ColumnSource> tables) {
        this.hashTable = hashTable;
        this.lookupExpressions = lookupExpressions;
        this.conditions = conditions;
        this.tables = tables;
    }

    public HashTable getHashTable() {
        return hashTable;
    }

    public List<ExpressionNode> getLookupExpressions() {
        return lookupExpressions;
    }

    public Collection<? extends ConditionExpression> getConditions() {
        return conditions;
    }

    public Collection<? extends ColumnSource> getTables() {
        return tables;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (tables != null) {
                for (ColumnSource table : tables) {
                    if (!table.accept(v))
                        break;
                }
            }
        }
        return v.visitLeave(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        tables = duplicateList(tables, map);
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

}
