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
package com.akiban.qp.expression;

import com.akiban.qp.operator.Bindings;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;

class NewExpressionWrapper implements com.akiban.qp.expression.Expression {

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public Expression get() {
        return delegate;
    }

    @Override
    public AkType getAkType() {
        return delegate.valueType();
    }

    NewExpressionWrapper(Expression delegate) {
        this.delegate = delegate;
    }

    private final com.akiban.server.expression.Expression delegate;
}
