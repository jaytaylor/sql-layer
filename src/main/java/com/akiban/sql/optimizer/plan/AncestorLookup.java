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

import java.util.List;
import java.util.ArrayList;

public class AncestorLookup extends BaseLookup
{
    private TableNode descendant;
    private List<TableNode> ancestors;

    public AncestorLookup(PlanNode input, TableNode descendant,
                          List<TableNode> ancestors,
                          List<TableSource> tables) {
        super(input, tables);
        this.descendant = descendant;
        this.ancestors = ancestors;
    }

    public AncestorLookup(PlanNode input, TableSource descendant,
                          List<TableSource> tables) {
        super(input, tables);
        this.descendant = descendant.getTable();
        this.ancestors = new ArrayList<TableNode>(tables.size());
        for (TableSource table : getTables()) {
            ancestors.add(table.getTable());
        }
    }

    public TableNode getDescendant() {
        return descendant;
    }

    public List<TableNode> getAncestors() {
        return ancestors;
    }

    @Override
    public String summaryString() {
        return super.summaryString() + "(" + descendant + " -> " + ancestors + ")";
    }

}
