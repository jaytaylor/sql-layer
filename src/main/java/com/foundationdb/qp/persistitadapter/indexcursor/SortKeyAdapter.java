/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.qp.persistitadapter.indexcursor;

import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.expression.BoundExpressions;
import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.expression.std.Comparison;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types3.TInstance;

public abstract class SortKeyAdapter<S, E> {
    public abstract AkType[] createAkTypes(int size);
    public abstract AkCollator[] createAkCollators(int size);
    public abstract TInstance[] createTInstances(int size);
    public abstract void setColumnMetadata(Column column, int f, AkType[] akTypes, AkCollator[] collators,
                                           TInstance[] tInstances);

    public abstract void checkConstraints(BoundExpressions loExpressions,
                                          BoundExpressions hiExpressions,
                                          int f,
                                          AkCollator[] collators,
                                          TInstance[] tInstances);

    public abstract S[] createSourceArray(int size);

    public abstract S get(BoundExpressions boundExpressions, int f);
    public abstract SortKeyTarget<S> createTarget();

    public abstract SortKeySource<S> createSource(TInstance tInstance);
    public abstract long compare(TInstance tInstance, S one, S two);
    public abstract E createComparison(TInstance tInstance, AkCollator collator, S one, Comparison comparison, S two);
    public abstract boolean evaluateComparison(E comparison, QueryContext queryContext);
    public boolean areEqual(TInstance tInstance, AkCollator collator, S one, S two, QueryContext queryContext) {
        E expr = createComparison(tInstance, collator, one, Comparison.EQ, two);
        return evaluateComparison(expr, queryContext);
    }

    public void checkConstraints(BoundExpressions loExpressions, BoundExpressions hiExpressions, TInstance[] instances,
                                 int f)
    {
        S loValueSource = get(loExpressions, f);
        S hiValueSource = get(hiExpressions, f);
        if (isNull(loValueSource) && isNull(hiValueSource)) {
            // OK, they're equal
        } else if (isNull(loValueSource) || isNull(hiValueSource)) {
            throw new IllegalArgumentException(String.format("lo: %s, hi: %s", loValueSource, hiValueSource));
        } else {
            TInstance tInstance = (instances == null) ? null : instances[f];
            long comparison = compare(tInstance, loValueSource, hiValueSource);
            if (comparison != 0) {
                throw new IllegalArgumentException();
            }
        }
    }

    public abstract boolean isNull(S source);

    public abstract S eval(Row row, int field);

    public abstract void setOrderingMetadata(Ordering ordering, int index,
                                             TInstance[] tInstances);
}
