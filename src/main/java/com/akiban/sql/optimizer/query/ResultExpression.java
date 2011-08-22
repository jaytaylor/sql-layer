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

package com.akiban.sql.optimizer.query;

/** An expression in a SELECT list. */
public class ResultExpression
{
    private BaseExpression expression;
    private String name;
    private boolean nameDefaulted;

    public ResultExpression(BaseExpression expression,
                            String name, boolean nameDefaulted) {
        this.expression = expression;
        this.name = name;
        this.nameDefaulted = nameDefaulted;
    }

    public BaseExpression getExpression() {
        return expression;
    }

    public String getName() {
        return name;
    }
    public boolean isNameDefaulted() {
        return nameDefaulted;
    }

    public String toString() {
        return expression.toString();
    }
}
