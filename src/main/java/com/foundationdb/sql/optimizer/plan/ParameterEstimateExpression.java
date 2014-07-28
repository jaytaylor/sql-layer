package com.foundationdb.sql.optimizer.plan;

import java.util.HashSet;
import java.util.Set;

import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.sql.types.DataTypeDescriptor;

public class ParameterEstimateExpression extends ParameterExpression {

    private Object value;

    public ParameterEstimateExpression(int position,
            DataTypeDescriptor sqlType, ValueNode sqlSource, TInstance type) {
        super(position, sqlType, sqlSource, type);
        setValue(null);
    }

    public ParameterEstimateExpression(int position, DataTypeDescriptor sqlType,
            ValueNode sqlSource, TInstance type, Object value) {
        super(position, sqlType, sqlSource, type);
        setValue(value);
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }
}
