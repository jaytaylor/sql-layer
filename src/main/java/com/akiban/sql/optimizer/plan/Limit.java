
package com.akiban.sql.optimizer.plan;

/** LIMIT / OFFSET */
public class Limit extends BasePlanWithInput
{
    private int offset, limit;
    private boolean offsetIsParameter, limitIsParameter;

    public Limit(PlanNode input,
                 int offset, boolean offsetIsParameter,
                 int limit, boolean limitIsParameter) {
        super(input);
        this.offset = offset;
        this.offsetIsParameter = offsetIsParameter;
        this.limit = limit;
        this.limitIsParameter = limitIsParameter;
    }

    public Limit(PlanNode input, int limit) {
        this(input, 0, false, limit, false);
    }

    public int getOffset() {
        return offset;
    }
    public boolean isOffsetParameter() {
        return offsetIsParameter;
    }
    public int getLimit() {
        return limit;
    }
    public boolean isLimitParameter() {
        return limitIsParameter;
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        if (offset > 0) {
            str.append("OFFSET ");
            if (offsetIsParameter) str.append("$");
            str.append(offset);
        }
        if (limit >= 0) {
            if (offset > 0) str.append(" ");
            str.append("LIMIT ");
            if (limitIsParameter) str.append("$");
            str.append(limit);
        }
        str.append(")");
        return str.toString();
    }

}
