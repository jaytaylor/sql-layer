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

import org.junit.runner.RunWith;
import com.akiban.server.types.ValueSource;
import com.akiban.junit.Parameterization;
import java.util.Collection;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.extract.Extractors;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.akiban.server.expression.std.WeekDayNameExpression.*;

@RunWith(NamedParameterizedRunner.class)
public class WeekDayNameExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    private AkType inputType;
    private AkType outputType;
    private String date;
    private String expected;
    private ExpressionComposer composer;

    public WeekDayNameExpressionTest (AkType inputType, AkType outputType, String date, String expected, ExpressionComposer composer)
    {
        this.inputType = inputType;
        this.outputType = outputType;
        this.date = date;
        this.expected = expected;
        this.composer = composer;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        // test day name
        String st1 = "", st2 = "";
        param(pb, AkType.DATE, AkType.VARCHAR, st1 = "2011-12-05", st2 = "Monday", DAYNAME_COMPOSER);
        param(pb, AkType.DATETIME, AkType.VARCHAR, st1 += " 12:30:10", st2, DAYNAME_COMPOSER);
        param(pb, AkType.TIMESTAMP, AkType.VARCHAR, st1, st2, DAYNAME_COMPOSER);
        param(pb, AkType.DATE, AkType.VARCHAR, null, null, DAYNAME_COMPOSER);
        
        // test day of week
        st1 = "2011-12-05";
        st2 = st1 + " 12:30:10";
        param(pb, AkType.DATE, AkType.INT, st1, "2", DAYOFWEEK_COMPOSER);
        param(pb, AkType.DATETIME, AkType.INT, st2, "2",DAYOFWEEK_COMPOSER);
        param(pb, AkType.TIMESTAMP, AkType.INT, st2, "2", DAYOFWEEK_COMPOSER);
        param(pb, AkType.DATETIME, AkType.INT, null, null, DAYOFWEEK_COMPOSER);
        param(pb, AkType.DATE, AkType.INT, "2011-12-35",null, DAYOFWEEK_COMPOSER);

        // test week day
        param(pb, AkType.DATE, AkType.INT, st1,"0", WEEKDAY_COMPOSER);
        param(pb, AkType.DATETIME, AkType.INT, st2,"0", WEEKDAY_COMPOSER);
        param(pb, AkType.TIMESTAMP, AkType.INT, st2,"0", WEEKDAY_COMPOSER);
        param(pb, AkType.TIMESTAMP, AkType.INT, null,null, WEEKDAY_COMPOSER);
        
        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb,AkType inputType, AkType outputType, String date, String expected, ExpressionComposer composer)
    {
        pb.add(composer + "(" + date + " [" + inputType + "]" + ") -->" +expected, inputType, outputType, date, expected, composer);
    }

    @Test
    public void test ()
    {
        Expression top = composer.compose(Arrays.asList(getExp(inputType, date)));
        ValueSource source = top.evaluation().eval();
        
        assertEquals(outputType, top.valueType());

        if (expected == null)
            assertTrue(".eval() is null ", source.isNull());
        else
            switch(outputType)
            {
                case INT:       assertEquals(Integer.parseInt(expected), source.getInt()); break;
                case VARCHAR:   assertEquals(expected, source.getString()); break;
                default:        assertTrue ("unexpected toptype", false);
            }
        alreadyExc = true;
    }

    private static Expression getExp (AkType type, String value)
    {
        if (value == null) return LiteralExpression.forNull();
        long l = Extractors.getLongExtractor(type).getLong(value);
        return new LiteralExpression(type, l);
    }

    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(1, AkType.DATE, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return composer;
    }

    @Override
    public boolean alreadyExc ()
    {
        return alreadyExc;
    }
}
