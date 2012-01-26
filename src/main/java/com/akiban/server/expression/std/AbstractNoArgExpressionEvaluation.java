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

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.util.ValueHolder;

public abstract class AbstractNoArgExpressionEvaluation implements ExpressionEvaluation {
    @Override
    public void of(Row row) {
    }

    @Override
    public void of(QueryContext context) {
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

    // for use by subclasses

    protected AbstractNoArgExpressionEvaluation() {
    }

    protected ValueHolder valueHolder () {
        return valueHolder == null ? valueHolder = new ValueHolder() : valueHolder;
    }

    private ValueHolder valueHolder;
}
