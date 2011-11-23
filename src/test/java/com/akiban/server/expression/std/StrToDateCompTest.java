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

import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;

public class StrToDateCompTest extends ComposedExpressionTestBase
{
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo (2, AkType.NULL, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return StrToDateExpression.COMPOSER;
    }

}
