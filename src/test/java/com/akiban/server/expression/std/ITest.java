/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.akiban.server.expression.std;

import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;
import com.akiban.server.types.extract.Extractors;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.Test;

/**
 *
 * @author Vy Thao Nguyen
 */
public class ITest
{
    @Test
    public void test()
    {
        Expression b = new LiteralExpression(AkType.DATE, 23L);

       double d = b.evaluation().eval().getLong();

    }

}
