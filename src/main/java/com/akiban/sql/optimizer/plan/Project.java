
package com.akiban.sql.optimizer.plan;

import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;

import java.util.List;

/** An list of expressions making up new rows. */
public class Project extends BasePlanWithInput implements ColumnSource, TypedPlan
{
    private List<ExpressionNode> fields;

    public Project(PlanNode input, List<ExpressionNode> fields) {
        super(input);
        this.fields = fields;
    }

    public List<ExpressionNode> getFields() {
        return fields;
    }

    @Override
    public String getName() {
        return "PROJECT";
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    for (int i = 0; i < fields.size(); i++) {
                        fields.set(i, fields.get(i).accept((ExpressionRewriteVisitor)v));
                    }
                }
                else if (v instanceof ExpressionVisitor) {
                    for (ExpressionNode field : fields) {
                        if (!field.accept((ExpressionVisitor)v))
                            break;
                    }
                }
            }
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString() {
        return super.summaryString() + fields;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        fields = duplicateList(fields, map);
    }

    @Override
    public int nFields() {
        return fields.size();
    }

    @Override
    public TInstance getTypeAt(int index) {
        ExpressionNode field = fields.get(index);
        TPreptimeValue tpv = field.getPreptimeValue();
        return tpv.instance();
    }

    @Override
    public void setTypeAt(int index, TPreptimeValue value) {
        fields.get(index).setPreptimeValue(value);
    }
}
