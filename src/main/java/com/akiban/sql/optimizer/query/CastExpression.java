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

package com.akiban.sql.optimizer.query;

import com.akiban.sql.StandardException;
import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.qp.expression.Expression;

public class CastExpression extends BaseExpression 
{
    private BaseExpression inner;

    public CastExpression(BaseExpression left, DataTypeDescriptor type) {
        super(type);
        this.inner = inner;
    }

    public String toString() {
        return "Cast(" + inner + " AS " + getType() + ")";
    }

    public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) 
            throws StandardException {
        // TODO: Need actual cast.
        return inner.generateExpression(fieldOffsets);
    }

}
