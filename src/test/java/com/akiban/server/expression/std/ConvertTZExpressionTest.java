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

package com.akiban.server.expression.std;

import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.ValueSource;
import com.akiban.server.expression.Expression;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
import com.akiban.server.types.extract.Extractors;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class ConvertTZExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    private static final LongExtractor extractor = Extractors.getLongExtractor(AkType.DATETIME);

    private final String dt;
    private final String from;
    private final String to;
    
    private final Long expected;
    public ConvertTZExpressionTest (String dt, String from, String to, Long exp)
    {
        this.dt = dt;
        this.from = from;
        this.to = to;
        expected = exp;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder b = new ParameterizationBuilder();
        
        param(b, "2009-12-12 00:00:00", "-06:00", "+5:00", 20091212110000L);
        param(b, "2004-01-01 12:00:00", "GMT", "MET", 20040101130000L);
        param(b, "2003-06-01 12:59:59", "-1:00", "+10:00", 20030601235959L);
        param(b, "2005-12-12", "foo", "bar", null);
        param(b, "00-00-01", "GMT", "MET", null);
        return b.asList();
    }

    private static void param(ParameterizationBuilder bd, String dt, String from, String to, Long exp)
    {
        bd.add("CONVET_TZ(" + dt + ", " + from + ", " + to + ")", dt, from, to, exp);
    }

    @Test
    public void test()
    {
        alreadyExc = true;
        Expression date = dt == null
                          ? LiteralExpression.forNull()
                          : new LiteralExpression(AkType.DATETIME, 
                                                 extractor.getLong(dt));
        
        Expression frm = from == null
                          ? LiteralExpression.forNull()
                          : new LiteralExpression(AkType.VARCHAR, from);
        
        Expression t = to == null
                       ? LiteralExpression.forNull()
                       : new LiteralExpression(AkType.VARCHAR, to);
        
        ValueSource top = new ConvertTZExpression(Arrays.asList(date, frm, t)).evaluation().eval();
        
        if (expected == null)
            assertTrue("Top should be NULL ", top.isNull());
        else
            assertEquals(extractor.asString(expected.longValue()), 
                         extractor.asString(top.getDateTime()));
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(3, AkType.DATETIME, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return ConvertTZExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
}
