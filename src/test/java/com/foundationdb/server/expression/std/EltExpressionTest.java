/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.junit.OnlyIf;
import com.foundationdb.junit.OnlyIfNot;
import com.foundationdb.junit.NamedParameterizedRunner;
import org.junit.runner.RunWith;
import org.junit.Test;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.util.ValueHolder;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static com.foundationdb.server.expression.std.ExprUtil.*;

@RunWith(NamedParameterizedRunner.class)
public class EltExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;;
    
    private List<? extends Expression> input;
    private ValueHolder expected;
    
    public EltExpressionTest (List<? extends Expression> in, ValueHolder exp)
    {
        input = in;
        expected = exp;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        param(pb, new ValueHolder(AkType.VARCHAR, "3"), lit(1L), lit("3"));
        param(pb, new ValueHolder(AkType.DOUBLE, 3.5), lit(2L), lit(3.0), lit(3.5));
        param(pb, new ValueHolder(AkType.DOUBLE, 3.5), lit(2L), lit(3.0), lit(3.5), constNull());
        param(pb, new ValueHolder(AkType.DATETIME, 20120318123010L), lit(3L), 
                       new LiteralExpression(AkType.DATETIME, 1L),
                       new LiteralExpression(AkType.DATETIME, 2L), 
                       new LiteralExpression(AkType.DATETIME, 20120318123010L));
        
        param(pb, ValueHolder.holdingNull(), constNull(), lit(2L));
        param(pb, ValueHolder.holdingNull(), lit(2L), lit("abc"), constNull());
        param(pb, ValueHolder.holdingNull(), lit(2L), lit("abc"));
        param(pb, ValueHolder.holdingNull(), lit(0), lit("a"));
        param(pb, ValueHolder.holdingNull(), lit(-5), lit("ab"));
        
        param(pb, null, lit(2));
        param(pb, null, null);
        param(pb, null);
        return pb.asList();
    }
    
    private static void param(ParameterizationBuilder pb, ValueHolder exp, Expression ... inputs)
    {
        pb.add("ELT(" + inputs + ")", inputs == null ? Collections.singletonList(constNull()):Arrays.asList(inputs), exp);
    }
    
    @OnlyIfNot("expectExc()")
    @Test
    public void testRegular()
    {   
        assertEquals("ELT(" + input + ") ", expected, new ValueHolder(new EltExpression(input).evaluation().eval()));
        alreadyExc = true;
    }
    
    @OnlyIf("expectExc()")
    @Test(expected=WrongExpressionArityException.class)
    public void testNegative()
    {
        compose(EltExpression.COMPOSER, input);
        alreadyExc = true;
    }
    
    public boolean expectExc()
    {
        return expected == null;
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.LONG, false);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return EltExpression.COMPOSER;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
    
}
