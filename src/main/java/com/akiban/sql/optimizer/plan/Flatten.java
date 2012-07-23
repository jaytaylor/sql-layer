/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
        return new HashSet<TableSource>(tableSources.subList(rightmostRight + 1,
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
        tableNodes = new ArrayList<TableNode>(tableNodes);
        tableSources = duplicateList(tableSources, map);
        joinTypes = new ArrayList<JoinType>(joinTypes);
    }

}
