package com.foundationdb.util;

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.server.types.value.ValueTargets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.ArrayList;
import java.util.List;

public class HashTable {



    private ArrayListMultimap<KeyWrapper, Row> hashTable = ArrayListMultimap.create();

    private RowType hashedRowType;
    private final HashFunction hashFunction = Hashing.goodFastHash(32); // Because we're returning longs


    public List<Row> getMatchingRows(Row row, List<TEvaluatableExpression> evaluatableComparisonFields, List<AkCollator> collators, QueryBindings bindings){
        return hashTable.get(new KeyWrapper(row, evaluatableComparisonFields, collators, bindings));
    }

    public void put(Row row, List<TEvaluatableExpression> evaluatableComparisonFields, List<AkCollator> collators, QueryBindings bindings){
        hashTable.put(new KeyWrapper(row, evaluatableComparisonFields, collators, bindings), row);
    }

    public RowType getRowType() {
        return hashedRowType;
    }

    public void setRowType(RowType rowType){
        hashedRowType = rowType;
    }

    public  class KeyWrapper{
        List<ValueSource> values = new ArrayList<>();
        Integer hashKey = 0;

        @Override
        public int hashCode(){
            return hashKey;
        }

        @Override
        public boolean equals(Object x) {
            if ( !(x instanceof KeyWrapper) ||  ((KeyWrapper)x).values.size() != values.size() )
                return false;
            for (int i = 0; i < values.size(); i++) {
                if(!ValueSources.areEqual(((KeyWrapper) x).values.get(i), values.get(i), values.get(i).getType()))
                    return false;
            }
            return true;
        }

        public KeyWrapper(Row row, List<TEvaluatableExpression> comparisonExpressions, List<AkCollator> collators, QueryBindings bindings){
            int i = 0;
            for( TEvaluatableExpression expression : comparisonExpressions) {
                if(row != null)
                    expression.with(row);
                if(bindings != null)
                    expression.with(bindings);
                expression.evaluate();
                ValueSource columnValue = expression.resultValue();
                Value valueCopy = new Value(columnValue.getType());
                ValueTargets.copyFrom(columnValue, valueCopy);
                AkCollator collator = (collators != null) ? collators.get(i++) : null;
                hashKey ^= ValueSources.hash(valueCopy, collator);
                values.add(valueCopy);
            }
        }
    }
}
