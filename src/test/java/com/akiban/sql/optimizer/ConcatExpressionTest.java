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

package com.akiban.sql.optimizer;

import com.akiban.server.expression.std.ConcatExpression;
import com.akiban.server.expression.std.ExpressionTypes;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.types.AkType;
import com.akiban.sql.StandardException;
import com.akiban.sql.optimizer.FunctionsTypeComputer.ArgumentsAccess;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ConcatExpressionTest
{
    @Test
    public void typeLength() throws StandardException
    {
        ArgList argLists = new ArgList(new DummyArgAccess(3));

        argLists.add(ExpressionTypes.varchar(6));
        argLists.add(ExpressionTypes.varchar(10));
        argLists.add(ExpressionTypes.varchar(4));

        ExpressionType concatType = ConcatExpression.COMPOSER.composeType(argLists);
        assertEquals(20, concatType.getPrecision());
    }

    private static class DummyArgAccess implements ArgumentsAccess
    {
        int size;
        protected DummyArgAccess (int size)
        {
            this.size = size;
        }

        @Override
        public int nargs()
        {
            return size;
        }

        @Override
        public ExpressionType argType(int index) throws StandardException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ExpressionType addCast(int index, ExpressionType argType, AkType requiredType) throws StandardException
        {
           throw new UnsupportedOperationException();
        }
    }
}
