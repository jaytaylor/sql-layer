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

package com.akiban.qp.row;

import com.akiban.qp.expression.ExpressionRow;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.util.ValueHolder;

import java.util.List;

public abstract class BindableRow {

    // BindableRow class interface

    public static BindableRow of(RowType rowType, List<? extends Expression> expressions) {
        for (Expression expression : expressions) {
            if (expression.needsBindings())
                return new BindingExpressions(rowType, expressions);
        }
        return new NonbindingExpressions(rowType, expressions);
    }

    public static BindableRow of(Row row) {
        return new Delegating(row);
    }

    // BindableRow instance interface

    public abstract Row bind(Bindings bindings);

    // nested classes

    private static class BindingExpressions extends BindableRow {
        @Override
        public Row bind(Bindings bindings) {
            return new ExpressionRow(rowType, bindings, expressions);
        }

        private BindingExpressions(RowType rowType, List<? extends Expression> expressions) {
            this.rowType = rowType;
            this.expressions = expressions;
            for (Expression expression : expressions) {
                if (expression.needsRow()) {
                    throw new IllegalArgumentException("expression " + expression + " needs a row");
                }
            }
        }

        private final List<? extends Expression> expressions;
        private final RowType rowType;
    }

    private static class NonbindingExpressions extends BindableRow {
        @Override
        public Row bind(Bindings bindings) {
            return row;
        }

        private NonbindingExpressions(RowType rowType, List<? extends Expression> expressions) {
            ValuesHolderRow holdersRow = new ValuesHolderRow(rowType);
            for (int i=0; i < expressions.size(); ++i) {
                Expression expression = expressions.get(i);
                ValueHolder holder = holdersRow.holderAt(i);
                if (expression.valueType() != rowType.typeAt(i)) {
                    throw new IllegalArgumentException(
                            "expressions[" + i + "] should have been type " + rowType.typeAt(i)
                                    + ", but was " + expression.valueType() + ": " + expressions);
                }
                if (expression.needsBindings())
                    throw new IllegalArgumentException("expressions[" + i +"] needs bindings: " + expressions);
                if (expression.needsRow())
                    throw new IllegalArgumentException("expressions[" + i +"] needs a row: " + expressions);
                holder.copyFrom(expression.evaluation().eval());
            }
            this.row = holdersRow;
        }

        private final Row row;
    }

    private static class Delegating extends BindableRow {
        @Override
        public Row bind(Bindings bindings) {
            return row;
        }

        private Delegating(Row row) {
            this.row = row;
        }

        private final Row row;
    }
}
