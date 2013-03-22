
package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.Routine;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

import java.util.List;

public class RoutineCondition extends RoutineExpression implements ConditionExpression
{
    public RoutineCondition(Routine routine,
                            List<ExpressionNode> operands,
                            DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(routine, operands, sqlType, sqlSource);
    }

    @Override
    public Implementation getImplementation() {
        return Implementation.NORMAL;
    }
}
