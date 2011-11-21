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


package com.akiban.server.expression.std;

import com.akiban.server.types.AkType;
import com.akiban.server.expression.Expression;
import org.junit.Test;
import static org.junit.Assert.*;

public class StrToDateExpressionTest
{
    @Test
    public void testDateStrToDate ()
    {
        Expression strExp = new LiteralExpression(AkType.VARCHAR, "2009-12-28");
        Expression frmExp = new LiteralExpression (AkType.VARCHAR, "%Y-%m-%d");

        Expression top = new StrToDateExpression(strExp, frmExp);
        assertTrue(AkType.DATE == top.evaluation().eval().getConversionType());
        assertEquals(2009L * 512 + 12L * 32 + 28L * 16, top.evaluation().eval().getDate());

    }

    @Test
    public void testDateStrToDate2 ()
    {
        Expression strExp = new LiteralExpression(AkType.VARCHAR, "2009abc12abc28");
        Expression frmExp = new LiteralExpression (AkType.VARCHAR, "%Yabc%mabc%d");

        Expression top = new StrToDateExpression(strExp, frmExp);
        assertTrue(AkType.DATE == top.evaluation().eval().getConversionType());
        assertEquals(2009L * 512 + 12L * 32 + 28L * 16, top.evaluation().eval().getDate());
    }

    // TODO: need more tests ,
}
