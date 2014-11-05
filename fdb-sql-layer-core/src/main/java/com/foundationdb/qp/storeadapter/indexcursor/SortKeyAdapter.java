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

package com.foundationdb.qp.storeadapter.indexcursor;

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.types.value.ValueRecord;
import com.foundationdb.qp.operator.API.Ordering;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.Comparison;

public abstract class SortKeyAdapter<S, E> {
    public abstract TInstance[] createTInstances(int size);
    public abstract void setColumnMetadata(Column column, int f, TInstance[] tInstances);

    public abstract void checkConstraints(ValueRecord loExpressions,
                                          ValueRecord hiExpressions,
                                          int f,
                                          AkCollator[] collators,
                                          TInstance[] tInstances);

    public abstract S[] createSourceArray(int size);

    public abstract S get(ValueRecord valueRecord, int f);
    public abstract SortKeyTarget<S> createTarget(Object descForError);

    public abstract SortKeySource<S> createSource(TInstance type);
    public abstract long compare(TInstance type, S one, S two);
    public abstract E createComparison(TInstance type, S one, Comparison comparison, S two);
    public abstract boolean evaluateComparison(E comparison, QueryContext queryContext);
    public boolean areEqual(TInstance type, S one, S two, QueryContext queryContext) {
        E expr = createComparison(type, one, Comparison.EQ, two);
        return evaluateComparison(expr, queryContext);
    }

    public abstract boolean isNull(S source);

    public abstract S eval(Row row, int field);

    public abstract void setOrderingMetadata(Ordering ordering, int index,
                                             TInstance[] tInstances);
}
