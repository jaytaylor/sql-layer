
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
