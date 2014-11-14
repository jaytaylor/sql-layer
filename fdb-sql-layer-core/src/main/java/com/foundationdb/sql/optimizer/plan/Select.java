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

/** Apply a conjunction of Boolean expressions to the input.
 */
public class Select extends BasePlanWithInput
{
    private ConditionList conditions;

    public Select(PlanNode input, ConditionList conditions) {
        super(input);
        this.conditions = conditions;
    }

    public ConditionList getConditions() {
        return conditions;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    conditions.accept((ExpressionRewriteVisitor)v);
                }
                else if (v instanceof ExpressionVisitor) {
                    conditions.accept((ExpressionVisitor)v);
                }
            }
        }
        return v.visitLeave(this);
    }
    
    @Override
    public String summaryString(SummaryConfiguration configuration) {
        return super.summaryString(configuration) + conditions.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        conditions = conditions.duplicate(map);
    }

}
