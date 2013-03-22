
package com.akiban.sql.optimizer.plan;

import com.akiban.server.types.AkType;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

public class ParameterCondition extends ParameterExpression 
                                implements ConditionExpression 
{
    public ParameterCondition(int position,
                              DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(position, sqlType, AkType.BOOL, sqlSource);
    }

    @Override
    public Implementation getImplementation() {
        return null;
    }

}
