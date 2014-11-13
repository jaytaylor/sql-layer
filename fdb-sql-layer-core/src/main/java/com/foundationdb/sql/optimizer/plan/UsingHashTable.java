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

/** A context with a Bloom filter. */
public class UsingHashTable extends UsingLoaderBase
{
    private HashTable hashTable;
    private List<ExpressionNode> lookupExpressions;
    private List<TKeyComparable> tKeyComparables;
    private List<AkCollator> collators;

    public UsingHashTable(HashTable hashTable, PlanNode loader, PlanNode input,
                          List<ExpressionNode> lookupExpressions,
                          List<TKeyComparable> tKeyComparables, List<AkCollator> collators) {
        super(loader, input);
        this.hashTable = hashTable;
        this.lookupExpressions = lookupExpressions;
        this.tKeyComparables = tKeyComparables;
        this.collators = collators;
    }

    public List<ExpressionNode> getLookupExpressions() {
        return lookupExpressions;
    }


    public HashTable getHashTable() {
        return hashTable;
    }
    public List<TKeyComparable> getTKeyComparables() {
        return tKeyComparables;
    }
    public List<AkCollator> getCollators() {
        return collators;
    }


    @Override
    public String summaryString(SummaryConfiguration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
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
    }

}