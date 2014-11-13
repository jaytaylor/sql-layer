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

import java.util.List;

import com.foundationdb.sql.optimizer.plan.ResultSet.ResultField;

import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;

/** A set operation on two input streams.(UNION, INTERSECT, EXCEPT )*/
public class SetPlanNode extends BasePlanNode implements PlanWithInput, TypedPlan, ColumnSource
{
    public enum opEnum { UNION, INTERSECT, EXCEPT, NULL }

    private PlanNode left, right;
    private boolean all;
    private List<ResultField> results;
    private String opName;
    private opEnum operationType = opEnum.NULL;


    public SetPlanNode(PlanNode left, PlanNode right, boolean all, String opName) {
        this.left = left;
        left.setOutput(this);
        this.right = right;
        right.setOutput(this);
        this.all = all;
        this.opName = opName;
    }

    public PlanNode getLeft() {
        return left;
    }

    public void setLeft(PlanNode left) {
        this.left = left;
        left.setOutput(this);
    }

    public PlanNode getRight() {
        return right;
    }

    public void setRight(PlanNode right) {
        this.right = right;
        right.setOutput(this);
    }

    public boolean isAll() {
        return all;
    }

    public List<ResultField> getResults() {
        return results;
    }

    public void setResults (List<ResultField> results) {
        this.results = results;
    }

    @Override
    public int nFields() {
        return results.size();
    }

    @Override
    public TInstance getTypeAt(int index) {
        return results.get(index).getType();
    }

    @Override
    public void setTypeAt(int index, TPreptimeValue value) {
        results.get(index).setType(value.type());
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        if (left == oldInput) {
            left = newInput;
            left.setOutput(this);
        }
        if (right == oldInput) {
            right = newInput;
            right.setOutput(this);
        }
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (left.accept(v))
                right.accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString(PlanToString.Configuration configuration) {
        StringBuilder sb = new StringBuilder(opName);
        sb.append('@');
        sb.append(Integer.toString(hashCode(), 16));
        if (all) {
            sb.append("(ALL)");
        }
        if (configuration.includeRowTypes) {
            sb.append('[');
            for (ResultField field : results) {
                sb.append(field);
                sb.append(" (");
                sb.append(field.getType());
                sb.append("), ");
            }
            if (results.size() > 0) {
                sb.setLength(sb.length()-2);
            }
            sb.append(']');
        }
        return sb.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        left = (PlanNode)left.duplicate(map);
        right = (PlanNode)right.duplicate(map);
    }

    public String getName(){
        return opName;
    }

    public opEnum getOperationType(){
        return operationType;
    }

    public void setOperationType(opEnum operationType){
        this.operationType = operationType;
    }




}
