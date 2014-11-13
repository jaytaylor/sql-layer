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

public class BranchLookup extends BaseLookup
{
    private TableNode source, ancestor, branch;

    public BranchLookup(PlanNode input, 
                        TableNode source, TableNode ancestor, TableNode branch,
                        List<TableSource> tables) {
        super(input, tables);
        this.source = source;
        this.ancestor = ancestor;
        this.branch = branch;
    }

    /** Lookup a branch right right beneath a starting point. */
    public BranchLookup(PlanNode input, TableNode source, List<TableSource> tables) {
        this(input, source, source, source, tables);
    }

    /** Lookup an immediate child of the starting point. */
    public BranchLookup(PlanNode input, TableNode source, TableNode branch, 
                        List<TableSource> tables) {
        this(input, source, source, branch, tables);
        assert(source == branch.getParent());
    }

    public TableNode getSource() {
        return source;
    }

    public TableNode getAncestor() {
        return ancestor;
    }

    public TableNode getBranch() {
        return branch;
    }

    @Override
    public String summaryString(PlanToString.Configuration configuration) {
        StringBuilder str = new StringBuilder(super.summaryString(configuration));
        str.append("(").append(source).append(" -> ").append(branch);
        if (ancestor != source)
            str.append(" via ").append(ancestor);
        str.append(")");
        return str.toString();
    }

}
