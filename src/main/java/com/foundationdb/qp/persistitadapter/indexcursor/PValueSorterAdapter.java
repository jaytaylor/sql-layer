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
package com.foundationdb.qp.persistitadapter.indexcursor;

import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.PersistitValuePValueSource;
import com.foundationdb.server.PersistitValuePValueTarget;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.pvalue.PValueSource;
import com.foundationdb.server.types.texpressions.TEvaluatableExpression;
import com.foundationdb.server.types.texpressions.TNullExpression;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.persistit.Value;

final class PValueSorterAdapter extends SorterAdapter<PValueSource, TPreparedExpression, TEvaluatableExpression> {
    @Override
    protected void appendDummy(API.Ordering ordering) {
        ordering.append(DUMMY_EXPRESSION, ordering.ascending(0));
    }

    @Override
    protected TInstance[] tinstances(int size) {
        return new TInstance[size];
    }


    @Override
    protected void initTypes(RowType rowType, TInstance[] tFieldTypes, int i) {
        tFieldTypes[i] = rowType.typeInstanceAt(i);
    }

    @Override
    protected void initTypes(API.Ordering ordering, int i, TInstance[] tInstances) {
        tInstances[i] = ordering.tInstance(i);
    }

    @Override
    protected TEvaluatableExpression evaluation(API.Ordering ordering, QueryContext context, QueryBindings bindings, int i) {
        TEvaluatableExpression evaluation = ordering.tExpression(i).build();
        evaluation.with(context);
        evaluation.with(bindings);
        return evaluation;
    }

    @Override
    protected PValueSource evaluateRow(TEvaluatableExpression evaluation, Row row) {
        evaluation.with(row);
        evaluation.evaluate();
        return evaluation.resultValue();
    }

    @Override
    protected void attachValueTarget(Value value) {
        valueTarget.attach(value);
    }

    @Override
    protected PersistitValueSourceAdapter createValueAdapter() {
        return new InternalPAdapter();
    }

    @Override
    protected void putFieldToTarget(PValueSource value, int i, TInstance[] tFieldTypes) {
        tFieldTypes[i].writeCanonical(value, valueTarget);
    }

    PValueSorterAdapter() {
        super(PValueSortKeyAdapter.INSTANCE);
    }
    
    private class InternalPAdapter implements PersistitValueSourceAdapter {

        @Override
        public void attach(Value value) {
            valueSource.attach(value);
        }

        @Override
        public void putToHolders(ValuesHolderRow row, int i, TInstance[] fieldTypes) {
            valueSource.getReady(fieldTypes[i]);
            fieldTypes[i].writeCanonical(valueSource, row.pvalueAt(i));
        }

        private final PersistitValuePValueSource valueSource = new PersistitValuePValueSource();
    }
    
    private final PersistitValuePValueTarget valueTarget = new PersistitValuePValueTarget();
    
    private static final TPreparedExpression DUMMY_EXPRESSION = new TNullExpression(MNumeric.BIGINT.instance(true));
}
