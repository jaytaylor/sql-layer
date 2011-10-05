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

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;

public final class TypedNullExpression implements Expression {

    // TypedNullExpression interface

    public static Expression ofConst(AkType type) {
        return new TypedNullExpression(type, true);
    }

    public static Expression nonConst(AkType type) {
        return new TypedNullExpression(type, false);
    }

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

    private TypedNullExpression(AkType type, boolean isConst) {
        this.type = type;
        this.isConst = isConst;
    }

    private final AkType type;
    private final boolean isConst;
}
