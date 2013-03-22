
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

import com.akiban.server.types.AkType;

/** An operation on Boolean expressions.
 */
public class BooleanOperationExpression extends BaseExpression 
                                        implements ConditionExpression
{
    public static enum Operation {
        AND("and"), OR("or"), NOT("not");

        private String functionName;
        
        Operation(String functionName) {
            this.functionName = functionName;
        }

        public String getFunctionName() {
            return functionName;
        }
    }

    private Operation operation;
    private ConditionExpression left, right;
    
    public BooleanOperationExpression(Operation operation, 
                                      ConditionExpression left, 
                                      ConditionExpression right, 
                                      DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, AkType.BOOL, sqlSource);
        this.operation = operation;
        this.left = left;
        this.right = right;
    }

    public Operation getOperation() {
        return operation;
    }
    public ConditionExpression getLeft() {
        return left;
    }
    public ConditionExpression getRight() {
        return right;
    }
    
    @Override
    public Implementation getImplementation() {
        return Implementation.NORMAL;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof BooleanOperationExpression)) return false;
        BooleanOperationExpression other = (BooleanOperationExpression)obj;
        return ((operation == other.operation) &&
                left.equals(other.left) &&
                right.equals(other.right));
    }

    @Override
    public int hashCode() {
        int hash = operation.hashCode();
        hash += left.hashCode();
        hash += right.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            if (left.accept(v))
                right.accept(v);
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
        left = (ConditionExpression)left.accept(v);
        right = (ConditionExpression)right.accept(v);
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        if (right == null)
            return operation + " " + left;
        else
            return left + " " + operation + " " + right;
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        left = (ConditionExpression)left.duplicate(map);
        right = (ConditionExpression)right.duplicate(map);
    }

}
