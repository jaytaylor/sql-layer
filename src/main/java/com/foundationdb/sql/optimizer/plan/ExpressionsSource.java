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

package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/** A join node explicitly enumerating rows.
 * From VALUES or IN.
 */
public class ExpressionsSource extends BaseJoinable implements ColumnSource, TypedPlan
{
    private List<List<ExpressionNode>> expressions;
    private TInstance[] tInstances;

    public ExpressionsSource(List<List<ExpressionNode>> expressions) {
        this.expressions = expressions;
    }

    public List<List<ExpressionNode>> getExpressions() {
        return expressions;
    }

    public List<TPreptimeValue> getPreptimeValues() {
        if (expressions.isEmpty())
            return Collections.emptyList();
        List<ExpressionNode> nodes = expressions.get(0);
        List<TPreptimeValue> result = new ArrayList<>(nodes.size());
        for (ExpressionNode node : nodes) {
            result.add(node.getPreptimeValue());
        }
        return result;
    }

    public void setTInstances(TInstance[] tInstances) {
        this.tInstances = tInstances;
    }

    public TInstance[] getFieldTInstances() {
        return tInstances;
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
        expressions = new ArrayList<>(expressions);
        for (int i = 0; i < expressions.size(); i++) {
            expressions.set(i, duplicateList(expressions.get(i), map));
        }
    }

    @Override
    public int nFields() {
        return expressions.get(0).size();
    }

    @Override
    public TInstance getTypeAt(int index) {
        return tInstances[index];
    }

    @Override
    public void setTypeAt(int index, TPreptimeValue value) {
        tInstances[index] = value.type();
    }
}
