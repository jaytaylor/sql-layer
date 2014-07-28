package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.sql.types.DataTypeDescriptor;

public class ParameterEstimateCondition extends ParameterEstimateExpression implements ConditionExpression {

    public ParameterEstimateCondition(int position,
            DataTypeDescriptor sqlType, ValueNode sqlSource,
            TInstance type) {
        super(position, sqlType, sqlSource, type);
    }

    @Override
    public Implementation getImplementation() {
        return null;
    }
}
