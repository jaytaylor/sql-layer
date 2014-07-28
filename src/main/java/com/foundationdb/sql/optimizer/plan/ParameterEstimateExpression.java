package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.sql.types.DataTypeDescriptor;

public class ParameterEstimateExpression extends ParameterExpression implements KnownValueExpression {

    private Object value;

    public ParameterEstimateExpression(int position,
            DataTypeDescriptor sqlType, ValueNode sqlSource, TInstance type) {
        super(position, sqlType, sqlSource, type);
        setPreptimeValue(ValueSources.fromObject(null, type));
    }

    public ParameterEstimateExpression(int position, DataTypeDescriptor sqlType,
            ValueNode sqlSource, TInstance type, Object value) {
        super(position, sqlType, sqlSource, type);
        setPreptimeValue(ValueSources.fromObject(value, type));
    }

    public void setValue(Object value) {
        setPreptimeValue(ValueSources.fromObject(value, getPreptimeValue().type()));
        this.value = value;
    }

    public Object getValue() {
        if (value == null) {
            ValueSource valueSource = getPreptimeValue().value();
            if (valueSource == null || valueSource.isNull())
                return null;
            value = ValueSources.toObject(valueSource);
        }
        return value;
    }
}
