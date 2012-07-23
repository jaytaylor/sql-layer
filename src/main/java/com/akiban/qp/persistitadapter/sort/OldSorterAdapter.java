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
import com.akiban.qp.row.ValuesHolderRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.PersistitValueValueSource;
import com.akiban.server.PersistitValueValueTarget;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.std.LiteralExpression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.server.types3.TInstance;
import com.persistit.Value;

public final class OldSorterAdapter extends SorterAdapter<ValueSource, Expression, ExpressionEvaluation> {
    @Override
    protected void appendDummy(API.Ordering ordering) {
        ordering.append(DUMMY_EXPRESSION, null, ordering.ascending(0));
    }

    @Override
    protected TInstance[] tinstances(int size) {
        return null;
    }

    @Override
    protected AkType[] aktypes(int size) {
        return new AkType[size];
    }

    @Override
    protected void initTypes(RowType rowType, AkType[] ofFieldTypes, TInstance[] tFieldTypes, int i) {
        ofFieldTypes[i] = rowType.typeAt(i);
    }

    @Override
    protected void initTypes(API.Ordering ordering, int i, AkType[] akTypes, TInstance[] tInstances) {
        akTypes[i] = ordering.type(i);
    }

    @Override
    protected ExpressionEvaluation evaluation(API.Ordering ordering, QueryContext context, int i) {
        ExpressionEvaluation evaluation = ordering.expression(i).evaluation();
        evaluation.of(context);
        return evaluation;
    }

    @Override
    protected ValueSource evaluateRow(ExpressionEvaluation evaluation, Row row) {
        evaluation.of(row);
        return evaluation.eval();
    }

    @Override
    protected PersistitValueSourceAdapater createValueAdapter() {
        return new InternalAdapter();
    }

    @Override
    protected void attachValueTarget(Value value) {
        valueTarget.attach(value);
    }

    @Override
    protected void putFieldToTarget(ValueSource value, int i, AkType[] oFieldTypes, TInstance[] tFieldTypes) {
        valueTarget.expectingType(oFieldTypes[i]);
        Converters.convert(value, valueTarget);
    }

    public OldSorterAdapter() {
        super(OldExpressionsSortKeyAdapter.INSTANCE);
    }
    
    private class InternalAdapter implements PersistitValueSourceAdapater {
        @Override
        public void attach(Value value) {
            valueSource.attach(value);
        }

        @Override
        public void putToHolders(ValuesHolderRow row, int i, AkType[] oFieldTypes, TInstance[] tFieldTypes) {
            ValueHolder valueHolder = row.holderAt(i);
            valueSource.expectedType(oFieldTypes[i]);
            valueHolder.copyFrom(valueSource);
        }

        private final PersistitValueValueSource valueSource = new PersistitValueValueSource();
    }
    
    private final PersistitValueValueTarget valueTarget =
            new PersistitValueValueTarget();

    private static final Expression DUMMY_EXPRESSION = LiteralExpression.forNull();
}
