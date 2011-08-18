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

import com.akiban.qp.physicaloperator.Bindings;
import com.akiban.qp.row.Row;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.FromObjectValueSource;

public final class ExpressionConversionHelper {

    // ExpressionConversionHelper interface

    public static ValueSource asConversionSource(Expression expression, Row row, Bindings bindings) {
        Object evaluated = expression.evaluate(row, bindings);
        FromObjectValueSource source = new FromObjectValueSource();
        source.setReflectively(evaluated);
        return source;
    }

    public static Object objectFromConversionSource(ValueSource source) {
        return new ToObjectValueTarget().convertFromSource(source);
    }

    private ExpressionConversionHelper() {}
}
