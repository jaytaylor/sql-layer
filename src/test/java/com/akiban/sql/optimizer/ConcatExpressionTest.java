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
import com.akiban.server.expression.TypesList;
import com.akiban.server.types.AkType;
import com.akiban.sql.StandardException;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ConcatExpressionTest
{
    @Test
    public void typeLength() throws StandardException
    {
        TypesList argLists = new DummyTypesList(3);

        argLists.add(ExpressionTypes.varchar(6));
        argLists.add(ExpressionTypes.varchar(10));
        argLists.add(ExpressionTypes.varchar(4));

        ExpressionType concatType = ConcatExpression.COMPOSER.composeType(argLists);
        assertEquals(20, concatType.getPrecision());
    }

    private static class DummyTypesList extends TypesList
    {
        DummyTypesList (int size)
        {
            super(size);      
        }

        @Override
        public void setType(int index, AkType newType) throws StandardException
        {
            // do nothing
        }

    }
}
