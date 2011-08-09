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
import com.akiban.server.types.ConversionSource;
import com.akiban.server.types.Converters;
import com.akiban.server.types.FromObjectConversionSource;
import com.akiban.server.types.ToObjectConversionTarget;

public final class ExpressionConversionHelper {

    // ExpressionConversionHelper interface

    public static ConversionSource asConversionSource(Expression expression, Row row, Bindings bindings) {
        Object evaluated = expression.evaluate(row, bindings);
        FromObjectConversionSource source = new FromObjectConversionSource();
        source.setReflectively(evaluated);
        return source;
    }

    public static Object objectFromConversionSource(ConversionSource source) {
        ToObjectConversionTarget target = new ToObjectConversionTarget();
        target.expectType( source.getConversionType() );
        Converters.convert(source, target);
        return target.lastConvertedValue();
    }

    private ExpressionConversionHelper() {}
}
