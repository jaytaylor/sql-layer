/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.sql.optimizer;

import com.foundationdb.server.expression.std.ConcatExpression;
import com.foundationdb.server.expression.std.ExpressionTypes;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.expression.TypesList;
import com.foundationdb.server.types.AkType;
import com.foundationdb.sql.StandardException;
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
