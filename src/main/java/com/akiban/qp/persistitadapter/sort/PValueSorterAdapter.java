/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.persistitadapter.sort;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.texpressions.TEvaluatableExpression;
import com.akiban.server.types3.texpressions.TNullExpression;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedLiteral;
import com.persistit.Value;

final class PValueSorterAdapter extends SorterAdapter<PValueSource, TPreparedExpression, TEvaluatableExpression> {
    @Override
    protected void appendDummy(API.Ordering ordering) {
        ordering.append(null, DUMMY_EXPRESSION, ordering.ascending(0));
    }

    @Override
    protected TInstance[] tinstances(int size) {
        return new TInstance[size];
    }

    @Override
    protected AkType[] aktypes(int size) {
        return null;
    }

    @Override
    protected void initTypes(RowType rowType, AkType[] ofFieldTypes, TInstance[] tFieldTypes, int i) {
        tFieldTypes[i] = rowType.typeInstanceAt(i);
    }

    @Override
    protected void initTypes(API.Ordering ordering, int i, AkType[] akTypes, TInstance[] tInstances) {
        tInstances[i] = ordering.tInstance(i);
    }

    @Override
    protected TEvaluatableExpression evaluation(API.Ordering ordering, QueryContext context, int i) {
        TEvaluatableExpression evaluation = ordering.tExpression(i).build();
        evaluation.with(context);
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
    protected PersistitValueSourceAdapater createValueAdapter() {
        throw new UnsupportedOperationException(); // TODO
    }

    @Override
    protected void putFieldToTarget(PValueSource value, int i, AkType[] oFieldTypes, TInstance[] tFieldTypes) {
        throw new UnsupportedOperationException(); // TODO
    }

    PValueSorterAdapter() {
        super(PValueSortKeyAdapter.INSTANCE);
    }
    
    private class InternalPAdapter implements PersistitValueSourceAdapater {
        private final PersistitValuePValueSource valueSource =
    }
    
    private final PersistitValuePValueTarget valueTarget
            = 
    
    private static final TPreparedExpression DUMMY_EXPRESSION = new TNullExpression(MNumeric.BIGINT.instance());
}
