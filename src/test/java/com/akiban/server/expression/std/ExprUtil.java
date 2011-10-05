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

final class ExprUtil {

    public static Expression lit(String string) {
        return new LiteralExpression(AkType.VARCHAR, string);
    }

    public static Expression lit(double value) {
        return new LiteralExpression(AkType.DOUBLE, value);
    }

    public static Expression lit(long value) {
        return new LiteralExpression(AkType.LONG, value);
    }

    public static Expression lit(boolean value) {
        return new LiteralExpression(AkType.BOOL, value);
    }

    public static Expression constNull() {
        return constNull(AkType.NULL);
    }

    public static Expression nonConstNull() {
        return nonConstNull(AkType.NULL);
    }

    public static Expression constNull(AkType type) {
        return new TypedNullExpression(type, true);
    }

    public static Expression nonConstNull(AkType type) {
        return new TypedNullExpression(type, false);
    }

    public static Expression exploding(AkType type) {
        return new ExplodingExpression(type);
    }

    private ExprUtil() {}

    private static final class TypedNullExpression implements Expression {

        // TypedNullExpression interface

        @Override
        public boolean isConstant() {
            return isConst;
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
            return LiteralExpression.forNull().evaluation();
        }

        @Override
        public AkType valueType() {
            return type;
        }

        // object interface

        @Override
        public String toString() {
            return "NULL(type=" + type + ')';
        }

        // use in this class

        TypedNullExpression(AkType type, boolean isConst) {
            this.type = type;
            this.isConst = isConst;
        }

        private final AkType type;
        private final boolean isConst;
    }

    private static final class ExplodingExpression implements Expression {

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

        ExplodingExpression(AkType type) {
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
}
