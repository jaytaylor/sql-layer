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

package com.foundationdb.server.expression.std;

import com.foundationdb.ais.model.Column;
import com.foundationdb.qp.exec.Plannable;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.row.Row;
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

/**
 * This is similar to a FieldExpression, except that fields are specified by an AIS Column, rather than
 * by RowType + int.
 */
public final class ColumnExpression implements Expression {

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
        return new InnerEvaluation(position);
    }

    @Override
    public AkType valueType() {
        return column.getType().akType();
    }

    // Object interface


    @Override
    public String toString() {
        return String.format("Field(%d)", position);
    }

    public ColumnExpression(Column column, int position) {
        this.column = column;
        this.position = position;
    }

    private final Column column;
    private final int position;

    @Override
    public String name()
    {
        return "FIELD";
    }

    @Override
    public CompoundExplainer getExplainer(ExplainContext context)
    {
        CompoundExplainer ex = new ExpressionExplainer(Type.FUNCTION, name(), context);
        ex.addAttribute(Label.BINDING_POSITION, PrimitiveExplainer.getInstance(position));
        return ex;
    }
    
    public boolean nullIsContaminating()
    {
        return true;
    }

    // nested classes

    private static class InnerEvaluation extends ExpressionEvaluation.Base {

        // ExpressionEvaluation interface

        @Override
        public void of(Row row) {
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
            return row.eval(position);
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

        private InnerEvaluation(int position) {
            this.position = position;
        }

        private final int position;
        private Row row;
    }
}
