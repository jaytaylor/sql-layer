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

import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/** A join node explicitly enumerating rows.
 * From VALUES or IN.
 */
public class ExpressionsSource extends BaseJoinable implements ColumnSource
{
    private List<List<ExpressionNode>> expressions;

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
        List<TPreptimeValue> result = new ArrayList<TPreptimeValue>(nodes.size());
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

    public TInstance[] getFieldTInstances() {
        if (expressions.isEmpty())
            return new TInstance[0];
        List<ExpressionNode> nodes = expressions.get(0);
        TInstance[] result = new TInstance[nodes.size()];
        for (int i=0; i < result.length; ++i) {
            result[i] = nodes.get(i).getPreptimeValue().instance();
        }
        return result;
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
        expressions = new ArrayList<List<ExpressionNode>>(expressions);
        for (int i = 0; i < expressions.size(); i++) {
            expressions.set(i, duplicateList(expressions.get(i), map));
        }
    }

}
