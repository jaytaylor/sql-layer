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

package com.foundationdb.server.expression.std;

import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.explain.std.ExpressionExplainer;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import java.util.List;
import java.util.Map;

public final class FieldExpression implements Expression {

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public boolean needsBindings() {
        return false;
    }

    @Override
    public boolean needsRow() {
        return true;
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return new InnerEvaluation(rowType, fieldIndex, valueType());
    }

    @Override
    public AkType valueType() {
        return rowType.typeAt(fieldIndex);
    }

    // Object interface


    @Override
    public String toString() {
        return String.format("Field(%d)", fieldIndex);
    }

    public FieldExpression(RowType rowType, int fieldIndex) {
        this.rowType = rowType;
        this.fieldIndex = fieldIndex;
        if (this.fieldIndex < 0 || this.fieldIndex >= rowType.nFields()) {
            throw new IllegalArgumentException("fieldIndex out of range: " + this.fieldIndex + " for " + this.rowType);
        }
    }

    private final RowType rowType;
    private final int fieldIndex;

    @Override
    public String name()
    {
        return "Field";
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new ExpressionExplainer(Type.FIELD, name(), context);
        ex.addAttribute(Label.ROWTYPE, rowType.getExplainer(context));
        ex.addAttribute(Label.POSITION, PrimitiveExplainer.getInstance(fieldIndex));
        if (context.hasExtraInfo(this))
            ex.get().putAll(context.getExtraInfo(this).get());
        return ex;
    }
    
    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    // nested classes

    private static class InnerEvaluation extends ExpressionEvaluation.Base {

        // ExpressionEvaluation interface

        @Override
        public void of(Row row) {
            RowType incomingType = row.rowType();
            if (!rowType.equals(incomingType)) {
                throw new IllegalArgumentException("wrong row type: " + incomingType + " != " + rowType);
            }
            ValueSource incomingSource = row.eval(fieldIndex);
            AkType incomingAkType = incomingSource.getConversionType();
            if (incomingAkType != AkType.NULL && !akType.equals(incomingAkType)) {
                throw new AkibanInternalException(
                        row + "[" + fieldIndex + "] had akType " + incomingAkType + "; expected " + akType
                );
            }
            this.row = row;
        }

        @Override
        public void of(QueryContext context) {
        }

        @Override
        public void of(QueryBindings bindings) {
        }

        @Override
        public ValueSource eval() {
            if (row == null)
                throw new IllegalStateException("haven't seen a row to target");
            return row.eval(fieldIndex);
        }

        // Shareable interface

        @Override
        public void acquire() {
            row.acquire();
        }

        @Override
        public boolean isShared() {
            return row.isShared();
        }

        @Override
        public void release() {
            row.release();
        }

        // private methods

        private InnerEvaluation(RowType rowType, int fieldIndex, AkType akType) {
            assert rowType != null;
            assert akType != null;
            this.rowType = rowType;
            this.fieldIndex = fieldIndex;
            this.akType = akType;
        }

        private final RowType rowType;
        private final int fieldIndex;
        private final AkType akType;
        private Row row;
    }
}
