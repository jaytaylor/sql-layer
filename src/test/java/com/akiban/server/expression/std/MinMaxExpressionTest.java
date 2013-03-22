

package com.akiban.server.expression.std;

import static com.akiban.server.expression.std.MinMaxExpression.Operation;
import org.junit.runner.RunWith;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.ValueSource;
import java.util.Collection;
import org.junit.Test;
import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class MinMaxExpressionTest extends ComposedExpressionTestBase
{
    private AkType type;
    private Object value1, value2, expected;
    private Operation operation;
    private static boolean alreadyExc = false;

    public MinMaxExpressionTest(AkType type, 
                                Object value1, Object value2, Object expected,
                                Operation operation)
    {
        this.type = type;
        this.value1 = value1;
        this.value2 = value2;
        this.expected = expected;
        this.operation = operation;
    }
    
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder ();
        
        pb.add("min int", AkType.INT, 123, 456, 123L, Operation.MIN);
        pb.add("max int", AkType.INT, 123, 456, 456L, Operation.MAX);

        pb.add("min string", AkType.VARCHAR, "123", "456", "123", Operation.MIN);

        pb.add("null", AkType.VARCHAR, "foo", null, null, Operation.MAX);

        return pb.asList();
    }
    
    @Test
    public void test()
    {
        Expression v1 = literal(value1);
        Expression v2 = literal(value2);
        Expression expression = new MinMaxExpression(v1, v2, operation);
        ValueSource result = expression.evaluation().eval();
        Object actual = null;
        if (result.isNull()) {
            assertTrue("Actual equals expected", (expected == null));
        }
        else {
            actual = new ToObjectValueTarget().expectType(type).convertFromSource(result);
            assertEquals("Actual equals expected", expected, actual);
        }
        alreadyExc = true;
    }

    private Expression literal(Object value) {
        switch (type) {
        case INT:
        case LONG:
            return new LiteralExpression(type, ((Number)value).longValue());
        case DOUBLE:
            return new LiteralExpression(type, ((Number)value).doubleValue());
        default:
            return new LiteralExpression(type, value);
        }
    }

    @Override
    protected CompositionTestInfo getTestInfo ()
    {
        return new CompositionTestInfo(2, type, true);
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        switch (operation) {
        case MIN:
            return MinMaxExpression.MIN_COMPOSER;
        case MAX:
            return MinMaxExpression.MAX_COMPOSER;
        default:
            return null;
        }
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }

}
