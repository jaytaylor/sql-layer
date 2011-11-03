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

import com.akiban.server.types.AkType;

import java.util.List;
import java.util.ArrayList;

/** A join node explicitly enumerating rows.
 * From VALUES or IN.
 */
public class ExpressionsSource extends BaseJoinable implements ColumnSource
{
    private List<List<ExpressionNode>> expressions;

    public ExpressionsSource(List<List<ExpressionNode>> expressions) {
        this.expressions = expressions;
    }

    public List<List<ExpressionNode>> getExpressions() {
        return expressions;
    }

    public AkType[] getFieldTypes() {
        if (expressions.isEmpty())
            return new AkType[0];
        List<ExpressionNode> nodes = expressions.get(0);
        AkType[] result = new AkType[nodes.size()];
        for (int i=0; i < result.length; ++i) {
            result[i] = nodes.get(i).getAkType();
        }
        return result;
    }

    // TODO: It might be interesting to note when it's also sorted for
    // WHERE x IN (...) ORDER BY x.
    public static enum DistinctState {
        DISTINCT, DISTINCT_WITH_NULL, HAS_PARAMETERS, HAS_EXPRESSSIONS,
        NEED_DISTINCT
    }

    private DistinctState distinctState;

    public DistinctState getDistinctState() {
        return distinctState;
    }
    public void setDistinctState(DistinctState distinctState) {
        this.distinctState = distinctState;
    }

    @Override
    public String getName() {
        return "VALUES";
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (v instanceof ExpressionRewriteVisitor) {
                for (List<ExpressionNode> row : expressions) {
                    for (int i = 0; i < row.size(); i++) {
                        row.set(i, row.get(i).accept((ExpressionRewriteVisitor)v));
                    }
                }                
            }
            else if (v instanceof ExpressionVisitor) {
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
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        str.append(expressions);
        if (distinctState != null) {
            switch (distinctState) {
            case NEED_DISTINCT:
                str.append(", ");
                str.append(distinctState);
                break;
            }
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        expressions = new ArrayList<List<ExpressionNode>>(expressions);
        for (int i = 0; i < expressions.size(); i++) {
            expressions.set(i, duplicateList(expressions.get(i), map));
        }
    }

}
