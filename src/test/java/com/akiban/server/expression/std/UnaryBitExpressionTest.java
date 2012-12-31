/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */


package com.akiban.server.expression.std;

import com.akiban.server.expression.std.UnaryBitExpression.UnaryBitOperator;
import org.junit.runner.RunWith;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.junit.Parameterization;
import java.util.Collection;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.server.types.ValueSource;
import java.math.BigInteger;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;

import com.akiban.server.types.NullValueSource;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public class UnaryBitExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }

    static interface Functors
    {
        ValueSource calc (BigInteger n);
        ValueSource error();
    }
    
    private static final Functors functor[] = new Functors[]
    {
        new Functors() 
        {
            @Override
            public ValueSource calc(BigInteger n)
            {
                return new ValueHolder(AkType.U_BIGINT, n.not().and(n64));
            }
            
            @Override 
            public ValueSource error()
            {
                return new ValueHolder(AkType.U_BIGINT,n64);
            }

            private final BigInteger n64 = new BigInteger("FFFFFFFFFFFFFFFF", 16);
        },
        new Functors()
        {
            @Override
            public ValueSource calc(BigInteger n)
            {
                 return new ValueHolder(AkType.LONG, n.signum() >= 0 ?  n.bitCount() : 64 - n.bitCount());
            }
            
            @Override 
            public ValueSource error()
            {
                return new ValueHolder(AkType.LONG, 0L);
            }
        }
    };
    
    private ExpressionComposer composer; 
    private UnaryBitOperator op;
    
    
    public UnaryBitExpressionTest (ExpressionComposer com, UnaryBitOperator op)
    {
        composer = com;
        this.op = op;
    }
    
    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        param(pb, UnaryBitExpression.BIT_COUNT_COMPOSER, UnaryBitOperator.COUNT);
        param(pb, UnaryBitExpression.NOT_COMPOSER, UnaryBitOperator.NOT);
        
        return pb.asList();
    }
    
    private static void param(ParameterizationBuilder pb, ExpressionComposer c,  UnaryBitOperator op)
    {
        pb.add(op.name() , c, op);
    }

    @Test
    public void testLong ()
    {
        Expression arg = new LiteralExpression(AkType.LONG, 15L);
        
        assertEquals(functor[op.ordinal()].calc(BigInteger.valueOf(15L)), getActualSource(arg));
        alreadyExc = true;
    }
    
    @Test 
    public void testDouble() 
    {
        Expression arg = new LiteralExpression(AkType.DOUBLE, 15.5);
        
        assertEquals(functor[op.ordinal()].calc(BigInteger.valueOf(16L)), getActualSource(arg));
    }
    
    @Test
    public void testBigInteger()
    {
        Expression arg = new LiteralExpression(AkType.U_BIGINT, new BigInteger("101011", 2));
        
        assertEquals(functor[op.ordinal()].calc(new BigInteger("101011",2)), getActualSource(arg));
    }
    
    @Test
    public void testString()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "15");
        
        assertEquals(functor[op.ordinal()].calc(BigInteger.valueOf(15)), getActualSource(arg));
    }
    
    @Test
    public void testNull ()
    {
        Expression arg = new LiteralExpression(AkType.NULL, null);
        
        assertEquals(NullValueSource.only(), getActualSource(arg));
    }
    
    @Test
    public void testNonNumeric()
    {
        Expression arg = new LiteralExpression(AkType.VARCHAR, "a");
        
        assertEquals(functor[op.ordinal()].error(), getActualSource(arg));
    }    
  
    private ValueSource getActualSource (Expression arg)
    {
        return compose(getComposer(), Arrays.asList(arg)).evaluation().eval();
    }
    
    @Override
    protected CompositionTestInfo getTestInfo() 
    {
        return testInfo;
    }

    @Override
    protected ExpressionComposer getComposer() 
    {
        return composer;
    }    
 
    private final CompositionTestInfo testInfo = new CompositionTestInfo(1, AkType.LONG, true);
}
