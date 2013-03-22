
package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public final class TruncateStringExpressionTest
{
    protected ValueSource truncate(ValueSource source, int length) {
        Expression expression = new TruncateStringExpression(length,
                                                             new LiteralExpression(source));
        return expression.evaluation().eval();
    }

    protected String truncate(String string, int length) {
        ValueSource source = new ValueHolder(AkType.VARCHAR, string);
        ValueSource result = truncate(source, length);
        return result.getString();
    }

    @Test
    public void testNull() {
        assertTrue("result is null", 
                   truncate(NullValueSource.only(), 128).isNull());
    }
    
    @Test
    public void testSame() {
        String value;

        value = "";
        assertEquals(value, truncate(value, 100));

        value = "some string";
        assertEquals(value, truncate(value, 100));

        value = "123";
        assertEquals(value, truncate(value, 3));
    }

    @Test
    public void testTruncate() {
        assertEquals("123",
                     truncate("123456", 3));
    }
}
