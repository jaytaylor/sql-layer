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
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.ExpressionEvaluation;

abstract public class AbstractVoidParamExpressionEvaluation implements ExpressionEvaluation
{
    protected AbstractVoidParamExpressionEvaluation (){} // do nothing

    @Override
    public void of(Row row) {} // do nothing

    @Override
    public void of(Bindings bindings) {} // do nothing

    @Override
    public void of(StoreAdapter adapter){} // do nothing

    @Override
    public void acquire() {} // do nothing

    @Override
    public boolean isShared() { return false; /*nothing to share? */ }

    @Override
    public void release() {} // do nothing
}
