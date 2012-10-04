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

import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.texpressions.TValidatedScalar;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;
import com.akiban.util.SparseArray;

import java.util.List;

/** A call to a function.
 */
public class FunctionExpression extends BaseExpression implements ResolvableExpression<TValidatedScalar>
{
    private String function;
    private List<ExpressionNode> operands;
    private TValidatedScalar overload;
    private SparseArray<Object> preptimeValues;
    private TPreptimeContext preptimeContext;

    public FunctionExpression(String function,
                              List<ExpressionNode> operands,
                              DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, sqlSource);
        this.function = function;
        this.operands = operands;
    }

    @Override
    public String getFunction() {
        return function;
    }
    public List<ExpressionNode> getOperands() {
        return operands;
    }

    @Override
    public void setResolved(TValidatedScalar resolved) {
        this.overload = resolved;
    }

    @Override
    public TValidatedScalar getResolved() {
        return overload;
    }

    public SparseArray<Object> getPreptimeValues() {
        return preptimeValues;
    }

    public void setPreptimeValues(SparseArray<Object> values) {
        this.preptimeValues = values;
    }

    @Override
    public TPreptimeContext getPreptimeContext() {
        return preptimeContext;
    }

    @Override
    public void setPreptimeContext(TPreptimeContext context) {
        this.preptimeContext = context;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof FunctionExpression)) return false;
        FunctionExpression other = (FunctionExpression)obj;
        return (function.equals(other.function) &&
                operands.equals(other.operands));
    }

    @Override
    public int hashCode() {
        int hash = function.hashCode();
        hash += operands.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            for (ExpressionNode operand : operands) {
                if (!operand.accept(v))
                    break;
            }
        }
        return v.visitLeave(this);
    }

    @Override
    public ExpressionNode accept(ExpressionRewriteVisitor v) {
        boolean childrenFirst = v.visitChildrenFirst(this);
        if (!childrenFirst) {
            ExpressionNode result = v.visit(this);
            if (result != this) return result;
        }
        for (int i = 0; i < operands.size(); i++) {
            operands.set(i, operands.get(i).accept(v));
        }
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(function);
        str.append("(");
        boolean first = true;
        for (ExpressionNode operand : operands) {
            if (first) first = false; else str.append(",");
            str.append(operand);
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        operands = duplicateList(operands, map);
    }

}
