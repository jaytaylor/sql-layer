
package com.akiban.sql.optimizer.plan;

import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/** A join node explicitly enumerating rows.
 * From VALUES or IN.
 */
public class ExpressionsSource extends BaseJoinable implements ColumnSource, TypedPlan
{
    private List<List<ExpressionNode>> expressions;
    private TInstance[] tInstances;

    public ExpressionsSource(List<List<ExpressionNode>> expressions) {
        this.expressions = expressions;
    }

    public List<List<ExpressionNode>> getExpressions() {
        return expressions;
    }

    public List<TPreptimeValue> getPreptimeValues() {
        if (expressions.isEmpty())
            return Collections.emptyList();
        List<ExpressionNode> nodes = expressions.get(0);
        List<TPreptimeValue> result = new ArrayList<>(nodes.size());
        for (ExpressionNode node : nodes) {
            result.add(node.getPreptimeValue());
        }
        return result;
    }

    public AkType[] getFieldTypes() {
        AkType[] result = null;
        for (List<ExpressionNode> nodes : expressions) {
            if (result == null)
                result = new AkType[nodes.size()];
            boolean incomplete = false;
            for (int i = 0; i < result.length; i++) {
                AkType type = nodes.get(i).getAkType();
                // Each type gets first non-null.
                // TODO: Should be UNIONed type, though maybe not computed here.
                if ((type == AkType.UNSUPPORTED) ||
                    (type == AkType.NULL)) {
                    incomplete = true;
                }
                if ((result[i] == null) ||
                    (result[i] == AkType.UNSUPPORTED) ||
                    (result[i] == AkType.NULL)) {
                    result[i] = type;
                }
            }
            if (!incomplete) break;
        }
        if (result == null)
            result = new AkType[0];
        return result;
    }

    public void setTInstances(TInstance[] tInstances) {
        this.tInstances = tInstances;
    }

    public TInstance[] getFieldTInstances() {
        return tInstances;
    }

    // TODO: It might be interesting to note when it's also sorted for
    // WHERE x IN (...) ORDER BY x.
    public static enum DistinctState {
        DISTINCT, DISTINCT_WITH_NULL, HAS_PARAMETERS, HAS_EXPRESSSIONS,
        NEED_DISTINCT
    }

    private DistinctState distinctState;

    public DistinctState getDistinctState() {
        return distinctState;
    }
    public void setDistinctState(DistinctState distinctState) {
        this.distinctState = distinctState;
    }

    @Override
    public String getName() {
        return "VALUES";
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (v instanceof ExpressionRewriteVisitor) {
                for (List<ExpressionNode> row : expressions) {
                    for (int i = 0; i < row.size(); i++) {
                        row.set(i, row.get(i).accept((ExpressionRewriteVisitor)v));
                    }
                }                
            }
            else if (v instanceof ExpressionVisitor) {
                expressions:
                for (List<ExpressionNode> row : expressions) {
                    for (ExpressionNode expr : row) {
                        if (!expr.accept((ExpressionVisitor)v)) {
                            break expressions;
                        }
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
        str.append(expressions);
        if (distinctState != null) {
            switch (distinctState) {
            case NEED_DISTINCT:
                str.append(", ");
                str.append(distinctState);
                break;
            }
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected boolean maintainInDuplicateMap() {
        return true;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        expressions = new ArrayList<>(expressions);
        for (int i = 0; i < expressions.size(); i++) {
            expressions.set(i, duplicateList(expressions.get(i), map));
        }
    }

    @Override
    public int nFields() {
        return expressions.get(0).size();
    }

    @Override
    public TInstance getTypeAt(int index) {
        return tInstances[index];
    }

    @Override
    public void setTypeAt(int index, TPreptimeValue value) {
        tInstances[index] = value.instance();
    }
}
