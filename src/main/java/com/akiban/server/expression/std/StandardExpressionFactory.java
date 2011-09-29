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

import com.akiban.server.error.NoSuchFunctionException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class StandardExpressionFactory implements ExpressionFactory {
    @Override
    public Expression compose(String name, List<? extends Expression> arguments) {
        ExpressionComposer composer = readOnlyComposers.get(name);
        if (composer == null)
            throw new NoSuchFunctionException(name);
        return composer.compose(arguments);
    }

    public StandardExpressionFactory() {
        this.readOnlyComposers = createComposers();
    }

    private final Map<String,ExpressionComposer> readOnlyComposers;

    private static Map<String,ExpressionComposer> createComposers() {
        Map<String,ExpressionComposer> result = new HashMap<String, ExpressionComposer>();
        // for now, just hard code these in. We'll make this more clever later.
        result.put("plus", LongOps.LONG_ADD);
        result.put("minus", LongOps.LONG_SUBTRACT);
        result.put("times", LongOps.LONG_MULTIPLY);

        return result;
    }
}
