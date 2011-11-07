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

public abstract class AbstractUnaryExpression implements Expression {

    // Expression interface

    @Override
    public boolean isConstant() {
        return operand.isConstant() || operand.valueType() == AkType.NULL;
    }

    @Override
    public boolean needsBindings() {
        return operand.needsBindings() && operand.valueType() != AkType.NULL;
    }

    @Override
    public boolean needsRow() {
        return operand.needsRow() && operand.valueType() != AkType.NULL;
    }

    @Override
    public AkType valueType() {
        return type;
    }

    // for use by subclasses

    protected abstract String name();

    protected final Expression operand() {
        return operand;
    }

    protected final ExpressionEvaluation operandEvaluation() {
        return operand().evaluation();
    }

    protected AbstractUnaryExpression(AkType type, Expression operand) {
        this.type = type;
        this.operand = operand;
    }

    // object interface

    @Override
    public String toString() {
        return name() + "(" + operand + ")";
    }

    // object state

    private final Expression operand;
    private final AkType type;
}
