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
import com.foundationdb.server.PersistitValueValueSource;
import com.foundationdb.server.PersistitValueValueTarget;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.std.LiteralExpression;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.conversion.Converters;
import com.foundationdb.server.types.util.ValueHolder;
import com.foundationdb.server.types3.TInstance;
import com.persistit.Value;

final class OldSorterAdapter extends SorterAdapter<ValueSource, Expression, ExpressionEvaluation> {
    @Override
    protected void appendDummy(API.Ordering ordering) {
        throw new IllegalStateException("types3 off");
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
        throw new IllegalStateException("types3 off");
    }

    @Override
    protected ExpressionEvaluation evaluation(API.Ordering ordering, QueryContext context, QueryBindings bindings, int i) {
        throw new IllegalStateException("types3 off");
    }

    @Override
    protected ValueSource evaluateRow(ExpressionEvaluation evaluation, Row row) {
        evaluation.of(row);
        return evaluation.eval();
    }

    @Override
    protected PersistitValueSourceAdapter createValueAdapter() {
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

    OldSorterAdapter() {
        super(OldExpressionsSortKeyAdapter.INSTANCE);
    }
    
    private class InternalAdapter implements PersistitValueSourceAdapter {
        @Override
        public void attach(Value value) {
            valueSource.attach(value);
        }

        @Override
        public void putToHolders(ValuesHolderRow row, int i, AkType[] oFieldTypes) {
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
