
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
