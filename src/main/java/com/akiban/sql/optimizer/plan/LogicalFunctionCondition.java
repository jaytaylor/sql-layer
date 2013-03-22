
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

import java.util.ArrayList;
import java.util.List;

public class LogicalFunctionCondition extends FunctionCondition
{
    // TODO: Can't use operands directly without making
    // FunctionExpression generic in <T extends ExpressionNode> for
    // its operands, because there's no other way to make accept
    // generic to indicate returning the type of this.
    public LogicalFunctionCondition(String function,
                                    List<ConditionExpression> operands,
                                    DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(function, new ArrayList<ExpressionNode>(operands), sqlType, sqlSource);
    }

    public ConditionExpression getOperand() {
        assert (getOperands().size() == 1);
        return (ConditionExpression)getOperands().get(0);
    }

    public ConditionExpression getLeft() {
        assert (getOperands().size() == 2);
        return (ConditionExpression)getOperands().get(0);
    }

    public ConditionExpression getRight() {
        assert (getOperands().size() == 2);
        return (ConditionExpression)getOperands().get(1);
    }

}
