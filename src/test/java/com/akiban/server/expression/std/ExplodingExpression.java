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

package com.akiban.server.expression.std;

import com.akiban.qp.operator.Bindings;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;

final class ExplodingExpression implements Expression {

    public static Expression of(AkType type) {
        return new ExplodingExpression(type);
    }

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
        return false;
    }

    @Override
    public ExpressionEvaluation evaluation() {
        return KILLER;
    }

    @Override
    public AkType valueType() {
        return type;
    }

    @Override
    public String toString() {
        return "EXPLODING(" + type + ')';
    }

    private ExplodingExpression(AkType type) {
        this.type = type;
    }

    private final AkType type;

    private static final ExpressionEvaluation KILLER = new ExpressionEvaluation() {
        @Override
        public void of(Row row) {
        }

        @Override
        public void of(Bindings bindings) {
        }

        @Override
        public ValueSource eval() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void acquire() {
        }

        @Override
        public boolean isShared() {
            return false;
        }

        @Override
        public void release() {
        }

        @Override
        public String toString() {
            return "EXPLOSION_EVAL";
        }
    };
}
