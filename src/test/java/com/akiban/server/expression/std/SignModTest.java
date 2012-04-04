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

import java.util.EnumSet;
import com.akiban.server.types.extract.Extractors;
import java.math.BigDecimal;
import java.math.BigInteger;
import com.akiban.server.expression.Expression;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.types.AkType;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith (NamedParameterizedRunner.class)
public class SignModTest
{
    private static class TestCase
    {
        public static final double EXPECTED = 3; // (-/+)3 % (-/+)5 == (-/+)3
        public static final double leftValue = 3;
        public static final double rightValue = 5;

        AkType leftType, rightType;
        int lsign, rsign;        

        public TestCase(AkType left, int lsign, AkType right, int rsign)
        {
            this.leftType = left;
            this.lsign = lsign;
            this.rightType = right;
            this.rsign = rsign;            
        }

        private static String toString (int num)
        {
            return num < 0 ? "-" : "+";
        }
        @Override
        public String toString ()
        {
            return toString(lsign) + leftType.name() + " MOD " +
                   toString(rsign) + rightType.name() + " ---> expect " + toString(lsign);
        }
    }

    private TestCase testCase;

    public SignModTest (TestCase testCase)
    {
        this.testCase = testCase;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        EnumSet<AkType> numerics = EnumSet.of(AkType.DOUBLE, AkType.LONG, AkType.FLOAT,
                                              AkType.INT, AkType.DECIMAL, AkType.U_BIGINT);

        for (AkType leftT : numerics)
            for (AkType rightT: numerics)
                param(pb, leftT, rightT);

        return pb.asList();
    }
    
    private static void param (ParameterizationBuilder pb, AkType left, AkType right)
    {
        int sign = 1;
        for (int count = 0; count < 2; ++count)
        {
            TestCase tc = new TestCase(left, sign, right, sign); // leftType and rightType have the same sign
            pb.add(tc.toString(), tc);

            tc = new TestCase(left, sign, right, sign *= -1); // leftType and rightType have different sign
            pb.add(tc.toString(),tc);
        }
    }

    @Test
    public void test ()
    {
        Expression left = getArg(testCase.leftType, TestCase.leftValue * testCase.lsign);
        Expression right = getArg(testCase.rightType, TestCase.rightValue * testCase.rsign);

        Expression top = ArithOps.MOD.compose(left, right);
        double actual = Extractors.getDoubleExtractor().getDouble(top.evaluation().eval());

        assertEquals(TestCase.EXPECTED * testCase.lsign, actual, 0.0001);
    }

    private static Expression getArg (AkType type, double num)
    {
        switch (type)
        {
            case FLOAT:     return new LiteralExpression(type, (float)num);
            case DOUBLE:    return new LiteralExpression(type, num);
            case INT:      
            case LONG:      return new LiteralExpression(type, (long)num);
            case U_BIGINT:  return new LiteralExpression(type, BigInteger.valueOf((long)num));
            case DECIMAL:   return new LiteralExpression (type, BigDecimal.valueOf(num));
            default:        throw new RuntimeException ("unexpected type");
        }
    }
}
