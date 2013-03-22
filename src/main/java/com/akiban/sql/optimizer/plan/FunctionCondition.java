
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

import java.util.List;

public class FunctionCondition extends FunctionExpression implements ConditionExpression
{
    public FunctionCondition(String function,
                             List<ExpressionNode> operands,
                             DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(function, operands, sqlType, sqlSource);
    }

    @Override
    public Implementation getImplementation() {
        return Implementation.NORMAL;
    }
}
