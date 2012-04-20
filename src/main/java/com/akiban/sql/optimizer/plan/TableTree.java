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

import com.akiban.ais.model.UserTable;

/** A subtree from the AIS.
 * In other words, a group.
 */
public class TableTree extends TableTreeBase<TableNode> 
{
    private int nbranches;

    protected TableNode createNode(UserTable table) {
        return new TableNode(table, this);
    }

    /** Determine branch sharing.
     * @return the number of branches. */
    public int colorBranches() {
        if (nbranches == 0)
            nbranches = colorBranches(root, 0);
        return nbranches;
    }

    private int colorBranches(TableNode node, int nbranches) {
        long branches = 0;
        for (TableNode child = node.getFirstChild(); 
             child != null; 
             child = child.getNextSibling()) {
            nbranches = colorBranches(child, nbranches);
            // A parent is on the same branch as any child.
            branches |= child.getBranches();
        }
        if (branches == 0) {
            // The leaf of a new branch.
            branches = (1L << nbranches++);
        }
        node.setBranches(branches);
        return nbranches;
    }

}
