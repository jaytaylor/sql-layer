package com.foundationdb.qp.util;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;

import java.util.ArrayList;
import java.util.List;

public  class KeyWrapper{
    List<ValueSource> values = new ArrayList<>();
    Integer hashKey = 0;

    @Override
    public int hashCode(){
        return hashKey;
    }

    @Override
    public boolean equals(Object x) {
        if (((KeyWrapper)x).values.size() != this.values.size())
            return false;
        for (int i = 0; i < values.size(); i++) {
            if (((KeyWrapper)x).values.get(i).equals(this.values.get(i)))
                return false;
        }
        return true;
    }

    public KeyWrapper(Row row, int comparisonFields[], List<AkCollator> collators){

        for (int f = 0; f < comparisonFields.length; f++) {
            ValueSource columnValue=row.value(comparisonFields[f]);
            AkCollator collator = (collators != null) ? collators.get(f) : null;
            hashKey = hashKey ^ ValueSources.hash(columnValue, collator);
            values.add(columnValue);
        }
    }
}
