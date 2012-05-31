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

/** Application of a Bloom filter. */
public class BloomFilterFilter extends BasePlanWithInput
{
    private BloomFilter bloomFilter;
    private PlanNode check;

    public BloomFilterFilter(BloomFilter bloomFilter, PlanNode input, PlanNode check) {
        super(input);
        this.bloomFilter = bloomFilter;
        this.check = check;
        check.setOutput(this);
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    public PlanNode getCheck() {
        return check;
    }
    public void setCheck(PlanNode check) {
        this.check = check;
        check.setOutput(this);
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        super.replaceInput(oldInput, newInput);
        if (check == oldInput) {
            check = newInput;
            check.setOutput(this);
        }
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            check.accept(v);
            getInput().accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        str.append(bloomFilter);
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        check = (PlanNode)check.duplicate(map);
    }

}
