
package com.akiban.sql.optimizer.plan;

import java.util.List;

/** Application of a Bloom filter. */
public class BloomFilterFilter extends BasePlanWithInput
{
    private BloomFilter bloomFilter;
    private List<ExpressionNode> lookupExpressions;
    private PlanNode check;

    public BloomFilterFilter(BloomFilter bloomFilter, List<ExpressionNode> lookupExpressions, 
                             PlanNode input, PlanNode check) {
        super(input);
        this.bloomFilter = bloomFilter;
        this.lookupExpressions = lookupExpressions;
        this.check = check;
        check.setOutput(this);
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }
    public List<ExpressionNode> getLookupExpressions() {
        return lookupExpressions;
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
            if (getInput().accept(v) && check.accept(v)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    for (int i = 0; i < lookupExpressions.size(); i++) {
                        lookupExpressions.set(i, lookupExpressions.get(i).accept((ExpressionRewriteVisitor)v));
                    }
                }
                else if (v instanceof ExpressionVisitor) {
                    for (ExpressionNode expr : lookupExpressions) {
                        if (!expr.accept((ExpressionVisitor)v))
                            break;
                    }
                }
            }
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        str.append(bloomFilter);
        str.append(", ");
        str.append(lookupExpressions);
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        check = (PlanNode)check.duplicate(map);
    }

}
