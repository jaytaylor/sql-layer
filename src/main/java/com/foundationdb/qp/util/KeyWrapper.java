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

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.server.types.value.ValueTargets;

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
        if ( !(x instanceof KeyWrapper) ||  ((KeyWrapper)x).values.size() != values.size() )
            return false;
        for (int i = 0; i < values.size(); i++) {
            if(!ValueSources.areEqual(((KeyWrapper)x).values.get(i), values.get(i), values.get(i).getType()))
                return false;
        }
        return true;
    }

    public KeyWrapper(Row row, List<TEvaluatableExpression> comparisonExpressions, List<AkCollator> collators){
        int i = 0;
        for( TEvaluatableExpression expression : comparisonExpressions) {
            expression.with(row);
            expression.evaluate();
            ValueSource columnValue = expression.resultValue();
            Value valueCopy = new Value(columnValue.getType());
            ValueTargets.copyFrom(columnValue, valueCopy);
            AkCollator collator = (collators != null) ? collators.get(i++) : null;
            hashKey = hashKey ^ ValueSources.hash(valueCopy, collator);
            values.add(valueCopy);
        }
    }
}
