/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;

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
                                    DataTypeDescriptor sqlType, ValueNode sqlSource,
                                    TInstance type) {
        super(function, new ArrayList<ExpressionNode>(operands), sqlType, sqlSource, type);
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
