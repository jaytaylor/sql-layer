
package com.akiban.sql.optimizer.plan;

import com.akiban.server.types.AkType;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

public class BooleanConstantExpression extends ConstantExpression 
                                       implements ConditionExpression 
{
    public BooleanConstantExpression(Object value, 
                                     DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(value, sqlType, AkType.BOOL, sqlSource);
    }

    public BooleanConstantExpression(Boolean value) {
        super(value, AkType.BOOL);
    }
    
    @Override
    public Implementation getImplementation() {
        return null;
    }

}
