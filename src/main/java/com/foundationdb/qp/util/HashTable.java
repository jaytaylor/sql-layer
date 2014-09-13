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

package com.foundationdb.qp.util;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TComparison;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.server.types.value.ValueTargets;
import com.google.common.collect.ArrayListMultimap;

import java.util.ArrayList;
import java.util.List;

public class HashTable {
    private ArrayListMultimap<KeyWrapper, Row> hashTable = ArrayListMultimap.create();

    private RowType hashedRowType;
    private List<TComparison> tComparisons;
    private List<AkCollator> collators;

    public List<Row> getMatchingRows(Row row, List<TEvaluatableExpression> evaluatableComparisonFields, QueryBindings bindings){
        return hashTable.get(new KeyWrapper(row, evaluatableComparisonFields, bindings));
    }

    public void put(Row row, List<TEvaluatableExpression> evaluatableComparisonFields, QueryBindings bindings){
        hashTable.put(new KeyWrapper(row, evaluatableComparisonFields, bindings), row);
    }

    public RowType getRowType() {
        return hashedRowType;
    }

    public void setRowType(RowType rowType) {
        hashedRowType = rowType;
    }

    public void setTComparisons(List<TComparison> tComparisons) {
        this.tComparisons =  tComparisons;
    }
    public void setCollators(List<AkCollator> collators) {
        this.collators =  collators;
    }

    public class KeyWrapper implements Comparable<KeyWrapper> {
        List<ValueSource> values = new ArrayList<>();
        int hashKey = 0;

        @Override
        public int hashCode() {
            return hashKey;
        }

        @Override
        public boolean equals(Object x) {
            if ( !(x instanceof KeyWrapper) ||  ((KeyWrapper)x).values.size() != values.size() )
                return false;
            return (compareTo((KeyWrapper)x) == 0);
        }

        @Override
        public int compareTo(KeyWrapper other) {
            for (int i = 0; i < values.size(); i++) {
                int compare;
                if (tComparisons != null && tComparisons.get(i) != null) {
                    compare = tComparisons.get(i).compare(values.get(i).getType(), values.get(i), other.values.get(i).getType(), other.values.get(i));
                }
                else {
                    compare = TClass.compare(values.get(i).getType(), values.get(i), other.values.get(i).getType(), other.values.get(i));
                }
                if (compare != 0) {
                    return compare;
                }
            }
            return 0;
        }

        public KeyWrapper(Row row, List<TEvaluatableExpression> comparisonExpressions, QueryBindings bindings){
            int i = 0;
            for (TEvaluatableExpression expression : comparisonExpressions) {
                if (row != null)
                    expression.with(row);
                if (bindings != null)
                    expression.with(bindings);
                expression.evaluate();
                ValueSource columnValue = expression.resultValue();
                Value valueCopy = new Value(columnValue.getType());
                ValueTargets.copyFrom(columnValue, valueCopy);
                AkCollator collator = (collators != null) ? collators.get(i) : null;
                hashKey ^= ValueSources.hash(valueCopy, collator);
                values.add(valueCopy);
                i++;
            }
        }
    }
}
