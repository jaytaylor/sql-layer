
package com.akiban.server.expression.std;

import com.akiban.server.error.OverflowException;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;

public final class ScaleDecimalExpressionTest
{
    protected ValueSource scale(ValueSource source, int precision, int scale) {
        Expression expression = new ScaleDecimalExpression(precision, scale,
                                                           new LiteralExpression(source));
        return expression.evaluation().eval();
    }

    protected BigDecimal scale(String str, int precision, int scale) {
        ValueSource source = new ValueHolder(AkType.DECIMAL, new BigDecimal(str));
        ValueSource result = scale(source, precision, scale);
        return result.getDecimal();
    }

    @Test
    public void testNull() {
        assertTrue("result is null", 
                   scale(NullValueSource.only(), 10, 2).isNull());
    }

    @Test(expected = OverflowException.class)
    public void testOverflow() {
        scale("12345.0", 5, 2);
    }
    
    @Test
    public void testScale() {
        assertEquals(new BigDecimal("1.23"),
                     scale("1.23", 4, 2));

        assertEquals(new BigDecimal("1.23"),
                     scale("1.229", 4, 2));

        assertEquals(new BigDecimal("1.20"),
                     scale("1.2", 4, 2));
    }
}
