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

public class BranchLookup extends BasePlanWithInput
{
    private TableSource source, branch;

    public BranchLookup(PlanNode input, TableSource source, TableSource branch) {
        super(input);
        this.source = source;
        this.branch = branch;
    }

    public TableSource getSource() {
        return source;
    }

    public TableSource getBranch() {
        return branch;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        source = (TableSource)source.duplicate();
        branch = (TableSource)branch.duplicate();
    }

    @Override
    public String summaryString() {
        return super.summaryString() +
            "(" + source.getTable() + " -> " + branch.getTable() + ")";
    }

}
