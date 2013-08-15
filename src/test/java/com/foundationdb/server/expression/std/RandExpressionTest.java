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

import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.types.AkType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.*;

public class RandExpressionTest
{
    
    private static List<Double> getRand (long seed, int size)
    {
        List<Double> ret = new ArrayList<>(size);
        Expression input = new LiteralExpression(AkType.LONG, seed);
        Expression top = new RandExpression(Arrays.asList(input));
        
        for (int n = 0; n < size; ++n)
            ret.add(top.evaluation().eval().getDouble());
        return ret;
    }
    
    private static void doTest(long seed, int size)
    {       
        List<Double> l1 = getRand(seed, size);
        List<Double> l2 = getRand(seed, size);
        
        for (int n = 0; n < size; ++n)
            assertEquals(l1.get(n), l2.get(n), 0.0001);
    }
    
    @Test
    public void test()
    {
        doTest(3, 5);
        doTest(1,10);
        doTest(6,2);
    }
}
