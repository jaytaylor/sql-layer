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
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.expression.Expression;
import com.akiban.util.ArgumentValidation;

import java.util.List;

public abstract class BindableRow {

    // BindableRow class interface

    public static BindableRow of(RowType rowType, List<? extends Expression> expressions) {
        ArgumentValidation.isEQ("rowType fields", rowType.nFields(), "expressions.size", expressions.size());
        for (Expression expression : expressions) {
            if (!expression.isConstant())
                return new BindingExpressions(rowType, expressions);
        }
        // all expressions are const; put them into a ValuesHolderRow
        ValuesHolderRow holderRow = new ValuesHolderRow(rowType);
        int i = 0;
        for (Expression expression : expressions) {
            holderRow.holderAt(i++).copyFrom(expression.evaluation().eval());
        }
        return new Delegating(holderRow);
    }

    public static BindableRow of(Row row) {
        return new Delegating(strictCopy(row));
    }

    // BindableRow instance interface

    public abstract Row bind(Bindings bindings, StoreAdapter adapter);

    private static Row strictCopy(Row input) {
        ValuesHolderRow copy = new ValuesHolderRow(input.rowType());
        for (int i=0; i < input.rowType().nFields(); ++i) {
            copy.holderAt(i).copyFrom(input.eval(i));
        }
        return copy;
    }

    // nested classes

    private static class BindingExpressions extends BindableRow {
        @Override
        public Row bind(Bindings bindings, StoreAdapter adapter) {
            return new ExpressionRow(rowType, bindings, adapter, expressions);
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

        // object interface


        @Override
        public String toString() {
            return "Bindable" + expressions;
        }

        private final List<? extends Expression> expressions;
        private final RowType rowType;
    }

    private static class Delegating extends BindableRow {
        @Override
        public Row bind(Bindings bindings, StoreAdapter adapter) {
            return row;
        }

        @Override
        public String toString() {
            return String.valueOf(row);
        }

        private Delegating(Row row) {
            this.row = row;
        }

        private final Row row;
    }
}
