
package com.akiban.sql.optimizer.plan;

import com.akiban.server.types.AkType;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

public class BooleanCastExpression extends CastExpression
                                   implements ConditionExpression
{
    public BooleanCastExpression(ExpressionNode inner, 
                                 DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(inner, sqlType, AkType.BOOL, sqlSource);
    }

    @Override
    public Implementation getImplementation() {
        return Implementation.NORMAL;
    }

}
