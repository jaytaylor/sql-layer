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

import com.akiban.qp.operator.Operator;
import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.qp.exec.Plannable;

import java.util.List;
import java.util.ArrayList;

/** Physical operator plan */
public abstract class BasePlannable extends BasePlanNode
{
    private Plannable plannable;
    private DataTypeDescriptor[] parameterTypes;
    
    protected BasePlannable(Plannable plannable,
                            DataTypeDescriptor[] parameterTypes) {
        this.plannable = plannable;
        this.parameterTypes = parameterTypes;
    }

    public Plannable getPlannable() {
        return plannable;
    }
    public DataTypeDescriptor[] getParameterTypes() {
        return parameterTypes;
    }

    public abstract boolean isUpdate();

    @Override
    public boolean accept(PlanVisitor v) {
        return v.visit(this);
    }


    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Do not copy operators.
    }

    public List<String> explainPlan() {
        List<String> result = new ArrayList<String>();
        explainPlan(plannable, result, 0);
        return result;
    }

    protected static void explainPlan(Plannable operator,
                                      List<String> into, int depth) {
            
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < depth; i++)
            sb.append("  ");
        sb.append(operator);
        into.add(sb.toString());
        for (Operator inputOperator : operator.getInputOperators()) {
            explainPlan(inputOperator, into, depth+1);
        }
    }
    
    @Override
    public String summaryString() {
        return withIndentedExplain(new StringBuilder(super.summaryString()));
    }

    @Override
    public String toString() {
        return withIndentedExplain(new StringBuilder(getClass().getSimpleName()));
    }

    protected String withIndentedExplain(StringBuilder str) {
        for (String operator : explainPlan()) {
            str.append("\n  ");
            str.append(operator);
        }
        return str.toString();
    }

}
