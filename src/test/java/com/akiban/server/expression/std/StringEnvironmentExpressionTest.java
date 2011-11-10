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

import com.akiban.server.expression.EnvironmentExpressionSetting;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.qp.operator.Bindings;
import com.akiban.qp.operator.ArrayBindings;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

public final class StringEnvironmentExpressionTest
{
    @Test
    public void test() {
        Bindings bindings = new ArrayBindings(0);
        List<Expression> expressions = new ArrayList<Expression>();
        List<String> values = new ArrayList<String>();
        int position = 0;
        for (EnvironmentExpressionSetting environmentSetting : EnvironmentExpressionSetting.values()) {
            String value = environmentSetting.toString();
            bindings.set(position, value);
            expressions.add(new StringEnvironmentExpression(environmentSetting, position));
            values.add(value);
            position++;
        }
        for (int i = 0; i < expressions.size(); i++) {
            ExpressionEvaluation eval = expressions.get(i).evaluation();
            eval.of(bindings);
            assertEquals(values.get(i), eval.eval().getString());
        }
    }
}
