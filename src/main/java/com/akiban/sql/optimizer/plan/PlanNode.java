
package com.akiban.sql.optimizer.plan;

public interface PlanNode extends PlanElement
{
    public PlanWithInput getOutput();

    public void setOutput(PlanWithInput output);

    public boolean accept(PlanVisitor v);

    public String summaryString();
}
