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
