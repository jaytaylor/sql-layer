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

package com.akiban.sql.optimizer.rule.range;

import com.akiban.server.types.AkType;
import com.akiban.sql.optimizer.plan.ConstantExpression;

public final class ExpressionBuilder {

    public static ConstantExpression constant(String value) {
        return new ConstantExpression(value, AkType.VARCHAR);
    }

    public static ConstantExpression constant(long value) {
        return new ConstantExpression(value, AkType.LONG);
    }

    private ExpressionBuilder() {}
}
