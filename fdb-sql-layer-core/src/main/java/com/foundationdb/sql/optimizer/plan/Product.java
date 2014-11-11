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

/** A product type join among several subplans. */
public class Product extends BasePlanNode implements PlanWithInput
{
    private TableNode ancestor;
    private List<PlanNode> subplans;

    public Product(TableNode ancestor, List<PlanNode> subplans) {
        this.ancestor = ancestor;
        this.subplans = subplans;
        for (PlanNode subplan : subplans)
          subplan.setOutput(this);
    }

    public TableNode getAncestor() {
        return ancestor;
    }

    public List<PlanNode> getSubplans() {
        return subplans;
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        int index = subplans.indexOf(oldInput);
        if (index >= 0) {
            subplans.set(index, newInput);
            newInput.setOutput(this);
        }
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            for (PlanNode subplan : subplans) {
                if (!subplan.accept(v))
                    break;
            }
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        if (ancestor != null) {
            str.append("(").append(ancestor).append(")");
        }
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        subplans = duplicateList(subplans, map);
    }

}
