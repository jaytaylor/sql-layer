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

import java.util.List;
import java.util.ArrayList;

/** This node has three phases:<ul>
 * <li>After parsing, it only knows the group by fields.</li>
 * <li>From analysis of downstream references, aggregate expressions are filled in.</li>
 * <li>After index select, but before maps are folded, a {@link Project}
 * is split off with input expressions, which are then forgotten.</li></ul>
 */
public class AggregateSource extends BasePlanWithInput implements ColumnSource
{
    public static enum Implementation {
        PRESORTED, PREAGGREGATE_RESORT, SORT, HASH, TREE, UNGROUPED,
        COUNT_STAR, COUNT_TABLE_STATUS, FIRST_FROM_INDEX
    }

    private boolean projectSplitOff;
    private List<ExpressionNode> groupBy;
    private List<AggregateFunctionExpression> aggregates;
    private List<Object> options;
    private int nGroupBy;
    private List<String> aggregateFunctions;
    
    private TableSource table;

    private Implementation implementation;

    public AggregateSource(PlanNode input,
                           List<ExpressionNode> groupBy) {
        super(input);
        this.groupBy = groupBy;
        nGroupBy = groupBy.size();
        if (!hasGroupBy())
            implementation = Implementation.UNGROUPED;
        aggregates = new ArrayList<AggregateFunctionExpression>();
        options = new ArrayList<Object>();
    }

    public boolean isProjectSplitOff() {
        return projectSplitOff;
    }

    public boolean hasGroupBy() {
        return (nGroupBy > 0);
    }

    public List<ExpressionNode> getGroupBy() {
        assert !projectSplitOff;
        return groupBy;
    }
    public List<AggregateFunctionExpression> getAggregates() {
        assert !projectSplitOff;
        return aggregates;
    }

    /** Add a new grouping field and return its position. */
    public int addGroupBy(ExpressionNode expr) {
        assert !projectSplitOff;
        int position = nGroupBy++;
        assert (position == groupBy.size());
        groupBy.add(expr);
        return position;
    }

    /** Add a new aggregate and return its position. */
    public int addAggregate(AggregateFunctionExpression aggregate) {
        int position = groupBy.size() + aggregates.size();
        aggregates.add(aggregate);
        options.add(aggregate.getOption());
        return position;
    }

    public ExpressionNode getField(int position) {
        assert !projectSplitOff;
        if (position < nGroupBy)
            return groupBy.get(position);
        else
            return aggregates.get(position - nGroupBy);
    }

    public int getNGroupBy() {
        return nGroupBy;
    }

    public int getNAggregates() {
        if (projectSplitOff)
            return aggregateFunctions.size();
        else
            return aggregates.size();
    }

    public int getNFields() {
        return getNGroupBy() + getNAggregates();
    }

    public List<String> getAggregateFunctions() {
        assert projectSplitOff;
        return aggregateFunctions;
    }

    public List<Object> getOptions()
    {
        assert projectSplitOff;
        return options;
    }

    public TableSource getTable() {
        assert (implementation == Implementation.COUNT_TABLE_STATUS);
        return table;
    }
    public void setTable(TableSource table) {
        assert (implementation == Implementation.COUNT_TABLE_STATUS);
        this.table = table;
    }

    public Implementation getImplementation() {
        return implementation;
    }
    public void setImplementation(Implementation implementation) {
        this.implementation = implementation;
    }

    public String getName() {
        return "GROUP";         // TODO: Something unique needed?
    }

    public List<ExpressionNode> splitOffProject() {
        List<ExpressionNode> result = new ArrayList<ExpressionNode>(groupBy);
        nGroupBy = groupBy.size();
        groupBy = null;
        aggregateFunctions = new ArrayList<String>(aggregates.size());
        for (AggregateFunctionExpression aggregate : aggregates) {
            String function = aggregate.getFunction();
            ExpressionNode operand = aggregate.getOperand();
            aggregate.setOperand(null);
            if (operand == null) {
                if ("COUNT".equals(function))
                    function = "COUNT(*)";
                operand = new ConstantExpression(1l, AkType.LONG);
            }
            aggregateFunctions.add(function);
            result.add(operand);
        }
        aggregates = null;
        projectSplitOff = true;
        return result;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (getInput().accept(v) && !projectSplitOff) {
                if (v instanceof ExpressionRewriteVisitor) {
                    for (int i = 0; i < groupBy.size(); i++) {
                        groupBy.set(i, groupBy.get(i).accept((ExpressionRewriteVisitor)v));
                    }
                    for (int i = 0; i < aggregates.size(); i++) {
                        aggregates.set(i, (AggregateFunctionExpression)aggregates.get(i).accept((ExpressionRewriteVisitor)v));
                    }
                }
                else if (v instanceof ExpressionVisitor) {
                    children:
                    {
                        for (ExpressionNode child : groupBy) {
                            if (!child.accept((ExpressionVisitor)v))
                                break children;
                        }
                        for (AggregateFunctionExpression child : aggregates) {
                            if (!child.accept((ExpressionVisitor)v))
                                break children;
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
        if (implementation != null) {
            str.append(implementation);
            str.append(",");
        }
        if (projectSplitOff) {
            if (hasGroupBy()) {
                str.append(nGroupBy);
                str.append(",");
            }
            str.append(aggregateFunctions);
        }
        else {
            if (hasGroupBy()) {
                str.append(groupBy);
                str.append(",");
            }
            str.append(aggregates);
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
        if (!projectSplitOff) {
            groupBy = duplicateList(groupBy, map);
            aggregates = duplicateList(aggregates, map);
        }
    }

}
