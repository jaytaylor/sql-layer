/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.akiban.server.expression.std;

import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.junit.Test;

public class ZNearExpressionTest {

    private static final Expression ZERO = new LiteralExpression(AkType.DOUBLE, 0.0);
    
    @Test (expected=UnsupportedSQLException.class)
    public void testNOP() {
        List<Expression> lst = new LinkedList<>(Arrays.asList(ZERO, ZERO, ZERO, ZERO));
        Expression exp = new ZNearExpression(lst);
        
        ValueSource source = exp.evaluation().eval();
    }
    
    @Test (expected=WrongExpressionArityException.class)
    public void testArity() {
        List<Expression> lst = new LinkedList<>(Arrays.asList(ZERO, ZERO));
        Expression exp = new ZNearExpression(lst);
    }
}
