package com.foundationdb.sql.optimizer.plan;

import java.util.HashSet;
import java.util.Set;

import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.sql.types.DataTypeDescriptor;

public class ParameterEstimateExpression extends ParameterExpression {

    private Set<Object> values;

    public ParameterEstimateExpression(int position,
            DataTypeDescriptor sqlType, ValueNode sqlSource, TInstance type) {
        super(position, sqlType, sqlSource, type);
        values = new HashSet<Object>();
    }

    public void setValues(Set<Object> values) {
        this.values.addAll(values);
    }

    public void addValue(Object value) {
        values.add(value);
    }
}
