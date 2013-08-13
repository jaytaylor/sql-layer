/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.expression.std;

import org.junit.runner.RunWith;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.junit.Parameterization;
import java.util.Collection;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.extract.Extractors;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.*;
import static com.foundationdb.server.expression.std.WeekDayNameExpression.*;

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
        Expression top = compose(composer, Arrays.asList(getExp(inputType, date)));
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
