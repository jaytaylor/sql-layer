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

import com.foundationdb.sql.optimizer.plan.JoinNode.JoinType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Take nested rows and join into single rowset. */
public class Flatten extends BasePlanWithInput
{
    // Must sometimes flatten in tables that aren't known to the
    // query, but are used as branchpoints for products.
    // This is the complete list.
    private List<TableNode> tableNodes;
    // This parallel list has nulls for those unknown tables.
    private List<TableSource> tableSources;
    // This list is one shorter and joins between each pair.
    private List<JoinType> joinTypes;

    public Flatten(PlanNode input, 
                   List<TableNode> tableNodes, 
                   List<TableSource> tableSources, 
                   List<JoinType> joinTypes) {
        super(input);
        this.tableNodes = tableNodes;
        this.tableSources = tableSources;
        this.joinTypes = joinTypes;
        assert (joinTypes.size() == tableSources.size() - 1);
    }

    public List<TableNode> getTableNodes() {
        return tableNodes;
    }

    public List<TableSource> getTableSources() {
        return tableSources;
    }

    public List<JoinType> getJoinTypes() {
        return joinTypes;
    }

    /** Get the tables involved in the sequence of inner joins, after
     * any RIGHTs and before any LEFTs. */
    public Set<TableSource> getInnerJoinedTables() {
        int rightmostRight = joinTypes.lastIndexOf(JoinType.RIGHT); // or -1
        int leftmostLeft = joinTypes.indexOf(JoinType.LEFT);
        if (leftmostLeft < 0)
            leftmostLeft = joinTypes.size();
        assert (rightmostRight < leftmostLeft);
        return new HashSet<>(tableSources.subList(rightmostRight + 1,
                                                             leftmostLeft + 1));
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        for (int i = 0; i < tableNodes.size(); i++) {
            if (i > 0) {
                str.append(" ");
                str.append(joinTypes.get(i-1));
                str.append(" ");
            }
            if (tableSources.get(i) != null)
                str.append(tableSources.get(i).getName());
            else
                str.append(tableNodes.get(i));
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        tableNodes = new ArrayList<>(tableNodes);
        tableSources = duplicateList(tableSources, map);
        joinTypes = new ArrayList<>(joinTypes);
    }

}
