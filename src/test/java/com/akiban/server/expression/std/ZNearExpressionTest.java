
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
