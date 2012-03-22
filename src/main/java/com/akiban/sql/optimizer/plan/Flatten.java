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

import com.akiban.sql.optimizer.plan.JoinNode.JoinType;

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

    /** Get the tables involved in an initial sequence of inner joins. */
    public Set<TableSource> getInnerJoinedTables() {
        Set<TableSource> result = new HashSet<TableSource>();
        for (int i = 0; i < tableSources.size(); i++) {
            if (i > 0) {
                if (joinTypes.get(i-1) != JoinType.INNER)
                    break;
            }
            TableSource table = tableSources.get(i);
            if (table != null)
                result.add(table);
        }
        return result;
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
        tableNodes = new ArrayList<TableNode>(tableNodes);
        tableSources = duplicateList(tableSources, map);
        joinTypes = new ArrayList<JoinType>(joinTypes);
    }

}
