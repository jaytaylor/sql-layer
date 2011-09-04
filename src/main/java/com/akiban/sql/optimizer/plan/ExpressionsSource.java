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

package com.akiban.sql.optimizer.plan;

import java.util.List;

/** A join node explicitly enumerating rows.
 * From VALUES or IN.
 */
public class ExpressionsSource extends BaseJoinable
{
    private List<List<ExpressionNode>> expressions;

    public ExpressionsSource(List<List<ExpressionNode>> expressions) {
        this.expressions = expressions;
    }

    public List<List<ExpressionNode>> getExpressions() {
        return expressions;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (v instanceof ExpressionVisitor) {
                expressions:
                for (List<ExpressionNode> row : expressions) {
                    for (ExpressionNode expr : row) {
                        if (!expr.accept((ExpressionVisitor)v)) {
                            break expressions;
                        }
                    }
                }
            }
        }
        return v.visitLeave(this);
    }
    
    @Override
    public String toString() {
        return super.toString() + expressions;
    }
}
